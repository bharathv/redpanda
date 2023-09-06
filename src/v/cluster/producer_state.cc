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

bool request::operator==(const request& other) const {
    bool range_match = _first_sequence == other._first_sequence
                       && _last_sequence == other._last_sequence;
    // both are in progress or both finished
    bool status_match = (in_progress() && other.in_progress())
                        || (!in_progress() && !other.in_progress());
    bool compare = range_match && status_match;
    if (compare && !in_progress()) {
        // both requests have finished
        // compare the result from promise;
        auto res = result.get_shared_future().get0();
        auto res_other = other.result.get_shared_future().get0();
        bool error_match = (res.has_error() && res_other.has_error())
                           || (!res.has_error() && !res_other.has_error());
        compare = compare && error_match;
        if (compare) {
            if (res.has_error()) {
                // both have errored out, compare errors
                compare = compare && res.error() == res_other.error();
            } else {
                // both finished, compared result offsets.
                compare = compare
                          && res.value().last_offset
                               == res_other.value().last_offset;
            }
        }
    }
    return compare;
}

bool requests::operator==(const requests& other) const {
    // check size match
    bool result
      = (_inflight_requests.size() == other._inflight_requests.size())
        && (_finished_requests.size() == other._finished_requests.size());
    if (!result) {
        return false;
    }
    // compare _inflight_requests
    auto it = _inflight_requests.begin(),
         other_it = other._inflight_requests.begin();
    for (; it != _inflight_requests.end(); ++it, ++other_it) {
        if (**it != **other_it) {
            return false;
        }
    }
    // compare _finished_requests
    it = _finished_requests.begin();
    other_it = other._finished_requests.begin();
    for (; it != _finished_requests.end(); ++it, ++other_it) {
        if (**it != **other_it) {
            return false;
        }
    }
    return true;
}

std::optional<request_ptr> requests::last_request() const {
    if (!_inflight_requests.empty()) {
        return _inflight_requests.back();
    } else if (!_finished_requests.empty()) {
        return _finished_requests.back();
    }
    return std::nullopt;
}

requests::request_result_t
requests::new_request(seq_t first, seq_t last, model::term_id current) {
    vlog(
      clusterlog.trace,
      "new request attempt, first: {}, last: {}, term: {}, producer: {}",
      first,
      last,
      current,
      *this);
    // gc any inflight requests from old terms
    while (!_inflight_requests.empty()
           && _inflight_requests.front()->_term < current
           && _inflight_requests.front()->in_progress()) {
        // Here we know for sure the term change, these in flight requests
        // are going to fail anyway, mark them so.
        _inflight_requests.front()->set_value(errc::not_leader);
        _inflight_requests.pop_front();
    }

    // check if an existing request matches
    auto match_it = std::find_if(
      _finished_requests.begin(),
      _finished_requests.end(),
      [first, last](const auto& request) {
          return request->_first_sequence == first
                 && request->_last_sequence == last;
      });

    if (match_it != _finished_requests.end()) {
        return match_it->get()->result.get_shared_future();
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
        return match_it->get()->result.get_shared_future();
    }

    // check if the incoming request forms a sequence.
    auto req = last_request();
    if (
      // there is a prev request, ensure sequence invariant is maintained.
      (req && req.value()->_last_sequence + 1 != first)
      // there is no prev request, incoming sequence number must be 0.
      || (!req && first != 0)) {
        return errc::sequence_out_of_order;
    }

    // All invariants satisified, enqueue the request.
    _inflight_requests.emplace_back(
      ss::make_lw_shared<request>(first, last, current, result_promise_t{}));

    return _inflight_requests.back();
}

bool requests::stm_apply(
  const model::batch_identity& bid, kafka::offset offset) {
    bool is_follower = false;
    auto first = bid.first_seq;
    auto last = bid.last_seq;
    if (!_inflight_requests.empty()) {
        auto front = _inflight_requests.front();
        if (front->_first_sequence == first && front->_last_sequence == last) {
            // Promote the request from in_flight -> finished.
            _inflight_requests.pop_front();
        }
    } else {
        is_follower = true;
    }
    result_promise_t ready{};
    ready.set_value(kafka_result{.last_offset = offset});
    _finished_requests.emplace_back(ss::make_lw_shared<request>(
      bid.first_seq, bid.last_seq, model::term_id{-1}, std::move(ready)));

    while (_finished_requests.size() > requests_cached_max) {
        _finished_requests.pop_front();
    }
    return is_follower;
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

producer_state::producer_state(
  producer_state_manager& mgr,
  ss::noncopyable_function<void()> hook,
  producer_state_snapshot snapshot) noexcept
  : _id(snapshot._id)
  , _group(snapshot._group)
  , _parent(std::ref(mgr))
  , _post_eviction_hook(std::move(hook)) {
    // Hydrate from snapshot.
    for (auto& req : snapshot._finished_requests) {
        result_promise_t ready{};
        ready.set_value(kafka_result{req._last_offset});
        _requests._finished_requests.push_back(ss::make_lw_shared<request>(
          req._first_sequence,
          req._last_sequence,
          model::term_id{-1},
          std::move(ready)));
    }
    register_self();
}

bool producer_state::operator==(const producer_state& other) const {
    return _id == other._id && _group == other._group
           && _evicted == other._evicted && _requests == other._requests;
}

std::ostream& operator<<(std::ostream& o, const requests& requests) {
    fmt::print(
      o,
      "{{ inflight: {}, finished: {} }}",
      requests._inflight_requests.size(),
      requests._finished_requests.size());
    return o;
}

std::ostream& operator<<(std::ostream& o, const producer_state& state) {
    fmt::print(
      o,
      "{{ id: {}, group: {}, requests: {}, "
      "ms_since_last_update: {}, evicted: {} }}",
      state._id,
      state._group,
      state._requests,
      state.ms_since_last_update(),
      state._evicted);
    return o;
}

ss::future<> producer_state::shutdown_input() {
    if (_evicted) {
        return ss::now();
    }
    _op_lock.broken();
    return _gate.close().then([this] { _requests.shutdown(); });
}

ss::future<> producer_state::evict() {
    if (_evicted) {
        return ss::now();
    }
    vlog(clusterlog.debug, "evicting producer: {}", *this);
    _evicted = true;
    vassert(_hook.is_linked(), "unexpected state, producer unlinked.");
    unlink_self();
    return shutdown_input().then_wrapped([this](auto result) {
        _post_eviction_hook();
        return result;
    });
}

void producer_state::register_self() {
    _parent.get().register_producer(*this);
    tickle();
}

void producer_state::deregister_self() {
    _parent.get().deregister_producer(*this);
}

void producer_state::unlink_self() {
    if (_hook.is_linked()) {
        _hook.unlink();
        vlog(clusterlog.trace, "unlink self: {}", *this);
    }
}

void producer_state::link_self() {
    if (_ops_in_progress == 0) {
        _parent.get().link(*this);
        vlog(clusterlog.trace, "link self: {}", *this);
    }
    tickle();
}

requests::request_result_t producer_state::enqueue_request(
  const model::batch_identity& bid, model::term_id current_term) {
    if (bid.first_seq > bid.last_seq) {
        // malformed batch
        return errc::invalid_request;
    }
    return _requests.new_request(bid.first_seq, bid.last_seq, current_term);
}

void producer_state::update(
  const model::batch_identity& bid, kafka::offset offset) {
    if (_gate.is_closed()) {
        return;
    }
    bool is_follower = _requests.stm_apply(bid, offset);
    if (is_follower) {
        // relink for LRU tracking on followers.
        // on leaders where this operation ran, it happens
        // in run_func() that relinks the producer.
        unlink_self();
        link_self();
    }
}

std::optional<seq_t> producer_state::last_sequence_number() const {
    auto maybe_ptr = _requests.last_request();
    if (!maybe_ptr) {
        return std::nullopt;
    }
    return maybe_ptr.value()->_last_sequence;
}

producer_state_snapshot
producer_state::snapshot(kafka::offset log_start_offset) const {
    producer_state_snapshot snapshot;
    snapshot._id = _id;
    snapshot._group = _group;
    snapshot._ms_since_last_update = ms_since_last_update();
    for (auto& req : _requests._finished_requests) {
        vassert(
          !req->in_progress(),
          "_finished_requests has unresolved promise: {}, range:[{}, {}]",
          *this,
          req->_first_sequence,
          req->_last_sequence);
        auto kafka_offset
          = req->result.get_shared_future().get().value().last_offset;
        // offsets older than log start are no longer interesting.
        if (kafka_offset >= log_start_offset) {
            snapshot._finished_requests.push_back(
              producer_state_snapshot::finished_request{
                ._first_sequence = req->_first_sequence,
                ._last_sequence = req->_last_sequence,
                ._last_offset = kafka_offset});
        }
    }
    return snapshot;
}

} // namespace cluster
