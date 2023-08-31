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

#pragma once

#include "cluster/types.h"
#include "model/record.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/mutex.h"
#include "utils/rwlock.h"

#include <seastar/core/shared_future.hh>
#include <seastar/util/defer.hh>

#include <bit>

// Befriended to expose internal state in tests.
struct test_fixture;

namespace cluster {

template<class Func>
concept AcceptsUnits = requires(Func f, ssx::semaphore_units units) {
    f(std::move(units));
};

class producer_state_manager;
class producer_state;
class request;

using producer_ptr = ss::lw_shared_ptr<producer_state>;
// Note: A shared_promise doesn't guarantee available() to be true
// right after set_value(), this is an implementation quirk, be
// mindful of that behavior when using it. We have a test for
// it in expiring_promise_test
using result_promise_t = ss::shared_promise<result<kafka_result>>;
using request_ptr = ss::lw_shared_ptr<request>;
using seq_t = int32_t;

enum class request_state : uint8_t { NEW = 0, IN_PROGRESS = 1, COMPLETED = 2 };

/// A request for a given sequence range, both inclusive.
/// The sequence numbers are stamped by the client and are a part
/// of batch header. A request can either be in progress or completed
/// depending on the whether the holding promise is set.
class request {
public:
    explicit request(
      seq_t first, seq_t last, model::term_id term, result_promise_t res)
      : _first_sequence(first)
      , _last_sequence(last)
      , _term(term)
      , _result(std::move(res)) {
        if (_result.available()) {
            _state = request_state::COMPLETED;
        }
    }

    template<class ValueType>
    void set_value(ValueType&& value) {
        vassert(
          _state <= request_state::IN_PROGRESS && !_result.available(),
          "unexpected request state during set: state: {}, result available: "
          "{}",
          static_cast<std::underlying_type_t<request_state>>(_state),
          _result.available());
        _result.set_value(std::forward<ValueType>(value));
        _state = request_state::COMPLETED;
    }
    void mark_request_in_progress() { _state = request_state::IN_PROGRESS; }
    request_state state() const { return _state; }
    result_promise_t::future_type result() const;

private:
    request_state _state{request_state::NEW};
    seq_t _first_sequence;
    seq_t _last_sequence;
    // term in which the request was submitted.
    model::term_id _term;
    // Set when the result for this request is finished. This is a shared
    // promise because a client can retry an already in progress request
    // (eg: timeouts) and we just chain the retried request request with
    // with the future from an already in-flight promise with the sequence
    // number match.
    result_promise_t _result;

    bool has_completed() { return _state == request_state::COMPLETED; }
    friend class requests;
    friend class producer_state;
};

// A cached buffer of requests, the requests can be in progress / finished.
// A request is promoted from inflight to finished once it is applied in the
// log.
//
// We retain a maximum of `requests_cached_max` finished requests.
// Kafka clients only issue requests in batches of 5, the queue is fairly small
// at all times.
class requests {
public:
    result<request_ptr> try_emplace(
      seq_t first, seq_t last, model::term_id current, bool reset_sequences);

    bool stm_apply(const model::batch_identity& bid, kafka::offset offset);

    void shutdown();

    friend std::ostream& operator<<(std::ostream&, const requests&);

private:
    static constexpr int32_t requests_cached_max = 5;
    // chunk size of the request containers to avoid wastage.
    static constexpr size_t chunk_size = std::bit_ceil(
      static_cast<unsigned long>(requests_cached_max));
    bool is_valid_sequence(seq_t incoming) const;
    std::optional<request_ptr> last_request() const;
    ss::chunked_fifo<request_ptr, chunk_size> _inflight_requests;
    ss::chunked_fifo<request_ptr, chunk_size> _finished_requests;
    friend producer_state;
};

/// Encapsulates all the state of a producer producing batches to
/// a single raft group. At init, the producer registers itself with
/// producer_state_manager that manages the lifecycle of all the
/// producers on a given shard.
class producer_state {
public:
    producer_state(
      producer_state_manager& mgr,
      model::producer_identity id,
      raft::group_id group,
      ss::noncopyable_function<void()> hook)
      : _id(id)
      , _group(group)
      , _parent(std::ref(mgr))
      , _last_updated_ts(ss::lowres_system_clock::now())
      , _post_eviction_hook(std::move(hook)) {
        register_self();
    }

    producer_state(const producer_state&) = delete;
    producer_state& operator=(producer_state&) = delete;
    producer_state(producer_state&&) noexcept = delete;
    producer_state& operator=(producer_state&& other) noexcept = delete;
    ~producer_state() noexcept { deregister_self(); }

    friend std::ostream& operator<<(std::ostream& o, const producer_state&);

    /// Runs the passed async function under the op_lock scope.
    /// Additionally does the following
    /// - de-registers self from the manager as a pre-hook
    /// - re-registers with the manager back after the function
    ///   completes
    /// This helps the manager implement a lock-free eviction approach.
    /// - A producer_state with an inflight request is not evicted, because it
    ///   is no longer in the list of producers tracked.
    /// - Re-registration helps the manager track LRU-ness, re-registration
    ///   effectively puts this producer at the end of the queue when
    ///   considering candidates for eviction.
    template<AcceptsUnits AsyncFunc>
    auto run_func(AsyncFunc&& func) {
        return ss::with_gate(
          _gate, [this, func = std::forward<AsyncFunc>(func)]() mutable {
              return _op_lock.get_units().then(
                [this, f = std::forward<AsyncFunc>(func)](auto units) {
                    unlink_self();
                    _ops_in_progress++;
                    return ss::futurize_invoke(f, std::move(units))
                      .then_wrapped([this](auto result) {
                          _ops_in_progress--;
                          link_self();
                          return result;
                      });
                });
          });
    }

    ss::future<> shutdown_input();
    ss::future<> evict();
    bool is_evicted() const { return _evicted; }

    /* reset sequences resets the tracking state and skips the sequence
     * checks.*/
    result<request_ptr> try_emplace_request(
      const model::batch_identity&,
      model::term_id current_term,
      bool reset_sequences = false);
    void update(const model::batch_identity&, kafka::offset);

private:
    // Register/deregister with manager.
    void register_self();
    void deregister_self();
    // Utilities to temporarily link and unlink from manager
    // without modifying the producer count.
    void link_self();
    void unlink_self();

    std::chrono::milliseconds ms_since_last_update() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
          ss::lowres_system_clock::now() - _last_updated_ts);
    }

    void touch() { _last_updated_ts = ss::lowres_system_clock::now(); }

    model::producer_identity _id;
    raft::group_id _group;
    // serializes all the operations on this producer
    mutex _op_lock;
    std::reference_wrapper<producer_state_manager> _parent;

    requests _requests;
    // Tracks the last time an operation is run with this producer.
    // Used to evict stale producers.
    ss::lowres_system_clock::time_point _last_updated_ts;
    intrusive_list_hook _hook;
    ss::gate _gate;
    // function hook called on eviction
    bool _evicted = false;
    size_t _ops_in_progress = 0;
    ss::noncopyable_function<void()> _post_eviction_hook;
    friend class producer_state_manager;
    friend struct ::test_fixture;
};
} // namespace cluster
