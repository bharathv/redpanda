/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "producer_state.h"

#include "cluster/logger.h"
#include "cluster/producer_state_manager.h"
#include "vassert.h"

namespace cluster {

std::optional<request_ptr>
requests::new_request(seq_t first, seq_t last, model::term_id current) {
    // gc any inflight requests from old terms
    while (!_inflight_requests.empty()
           && _inflight_requests.front()->_term < current) {
        auto front = _inflight_requests.front();
        if (front->in_progress()) {
            front->result.set_value(errc::not_leader);
        }
        _inflight_requests.pop_front();
    }

    // check if an existing request matches
    auto match_it = std::find_if(
      _finished_requests.begin(),
      _finished_requests.end(),
      [first, last, current](const auto& request) {
          return request->_first_sequence == first
                 && request->_last_sequence == last
                 && request->_term == current;
      });

    if (match_it != _finished_requests.end()) {
        return *match_it;
    }

    match_it = std::find_if(
      _inflight_requests.begin(),
      _inflight_requests.end(),
      [first, last, current](const auto& request) {
          return request->_first_sequence == first
                 && request->_last_sequence == last
                 && request->_term == current;
      });

    if (match_it != _inflight_requests.end()) {
        return *match_it;
    }

    // check if the incoming request forms a sequence.
    auto req = last_request();
    if (
      (req && req.value()->_last_sequence + 1 != first)
      || (!req && first != 0)) {
        return std::nullopt;
    }

    // All invariants satisified, enqueue the request.
    _inflight_requests.emplace_back(
      ss::make_lw_shared<request>(first, last, current, result_promise_t{}));

    return _inflight_requests.back();
}

void requests::stm_apply(
  const model::batch_identity& bid, kafka::offset offset) {
    auto first = bid.first_seq;
    auto last = bid.last_seq;
    if (!_inflight_requests.empty()) {
        auto front = _inflight_requests.front();
        if (front->_first_sequence == first && front->_last_sequence == last) {
            _inflight_requests.pop_front();
        }
    }
    result_promise_t ready{};
    ready.set_value(kafka_result{.last_offset = offset});
    _finished_requests.emplace_back(ss::make_lw_shared<request>(
      bid.first_seq, bid.last_seq, model::term_id{-1}, std::move(ready)));

    while (_finished_requests.size() > requests_cached_max) {
        _finished_requests.pop_front();
    }
}

void requests::shutdown() {
    for (auto request : _inflight_requests) {
        if (request->in_progress()) {
            request->result.set_value(errc::shutting_down);
        }
    }
    _inflight_requests.clear();
    _finished_requests.clear();
}

producer_state::producer_state(producer_state&& other) noexcept
  : _id(other._id)
  , _group(other._group)
  , _op_lock(std::move(other._op_lock))
  , _parent(other._parent)
  , _requests(std::move(other._requests))
  , _eviction_hook(std::move(other._eviction_hook)) {
    _hook.swap_nodes(other._hook);
}

producer_state& producer_state::operator=(producer_state&& other) noexcept {
    if (this != &other) {
        _id = other._id;
        _group = other._group;
        _op_lock = std::move(other._op_lock);
        _parent = other._parent;
        _requests = std::move(other._requests);
        _eviction_hook = std::move(other._eviction_hook);
        _hook.swap_nodes(other._hook);
    }
    return *this;
}

void producer_state::evict() {
    if (!_evicted) {
        vassert(_hook.is_linked(), "unexpected state, producer unlinked.");
        unlink_self();
        _eviction_hook();
        _evicted = true;
    }
}

void producer_state::regist3r() {
    _parent.get().regist3r(*this);
    tickle();
}

void producer_state::deregist3r() { _parent.get().deregist3r(*this); }

void producer_state::unlink_self() {
    vassert(!_hook.is_linked(), "producer {} is not already linked", _id);
    _hook.unlink();
}

void producer_state::link_self() {
    _parent.get().link(*this);
    tickle();
}

producer_state::enqueue_result producer_state::enqueue_request(
  const model::batch_identity& bid, model::term_id current_term) {
    if (bid.first_seq > bid.last_seq) {
        // malformed batch
        return errc::invalid_request;
    }
    auto request = _requests.new_request(
      bid.first_seq, bid.last_seq, current_term);
    if (request) {
        return request.value();
    }
    return errc::sequence_out_of_order;
}

void producer_state::update(
  const model::batch_identity& bid, kafka::offset offset) {
    if (_gate.is_closed()) {
        return;
    }
    _requests.stm_apply(bid, offset);
}

ss::future<> producer_state::shutdown() {
    _op_lock.broken();
    co_await _gate.close();
    _requests.shutdown();
}

} // namespace cluster
