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

producer_state::producer_state(producer_state&& other) noexcept
  : _id(other._id)
  , _group(other._group)
  , _term(other._term)
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
        _term = other._term;
        _op_lock = std::move(other._op_lock);
        _parent = other._parent;
        _requests = std::move(other._requests);
        _eviction_hook = std::move(other._eviction_hook);
        _hook.swap_nodes(other._hook);
    }
    return *this;
}

void producer_state::deregister_self() {
    vassert(
      _hook.is_linked(),
      "producer state is not linked, unexpeted state, this is am "
      "implementation bug");
    _parent.get().deregister_proudcer(*this);
}

void producer_state::register_self() {
    vassert(!_hook.is_linked(), "producer state already linked with mgr.");
    _parent.get().register_producer(*this);
    tickle();
}

producer_state::enqueue_result producer_state::enqueue_request(
  const model::batch_identity& bid, model::term_id current_term) {
    if (bid.first_seq > bid.last_seq) {
        return errc::invalid_request;
    }
    if (_term < current_term) {
        for (auto& request : _requests) {
            if (request->in_progress()) {
                request->result.set_value(errc::generic_tx_error);
            }
        }
        _requests.clear();
        _term = current_term;
    } else if (_term > current_term) {
        return errc::not_leader;
    }
    // Check if the request is in progress or has already been finished.
    auto match_it = std::find_if(
      _requests.begin(), _requests.end(), [&](const auto& request) {
          return request->first_sequence == bid.first_seq
                 && request->last_sequence == bid.last_seq;
      });
    if (match_it != _requests.end()) {
        return (*match_it)->result.get_shared_future();
    }
    // New request, check for in order sequence.
    if (_requests.empty()) {
        if (bid.first_seq != 0) {
            // First sequence begins with 0.
            return errc::sequence_out_of_order;
        }
    } else {
        const auto& last = _requests.back();
        if (last->last_sequence + 1 != bid.first_seq) {
            return errc::sequence_out_of_order;
        }
    }
    // All invariants satisified, enqueue the request.
    _requests.emplace_back(ss::make_lw_shared<request>(
      bid.first_seq, bid.last_seq, shared_promise_t{}));
    return _requests.back();
}

void producer_state::update(
  const model::batch_identity& bid, kafka::offset offset) {
    if (_gate.is_closed()) {
        return;
    }
    if (_requests.empty() || _requests.front()->last_sequence < bid.first_seq) {
        // follower.
        shared_promise_t ready{};
        ready.set_value(kafka_result{.last_offset = offset});
        _requests.emplace_back(ss::make_lw_shared<request>(
          bid.first_seq, bid.last_seq, std::move(ready)));
    }
    while (_requests.size() > requests_cached_max
           && !_requests.front()->in_progress()) {
        _requests.pop_front();
    }
}

ss::future<> producer_state::shutdown() {
    _op_lock.broken();
    co_await _gate.close();
    for (auto& request : _requests) {
        if (request->in_progress()) {
            request->result.set_value(errc::shutting_down);
        }
    }
    _requests.clear();
}

} // namespace cluster
