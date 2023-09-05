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
using result_promise_t = ss::shared_promise<result<kafka_result>>;
using request_ptr = ss::lw_shared_ptr<request>;
using seq_t = int32_t;

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
      , result(std::move(res)) {}

    template<class ValueType>
    void set_value(ValueType&& value) {
        result.set_value(std::forward<ValueType>(value));
    }

private:
    seq_t _first_sequence;
    seq_t _last_sequence;
    // term in which the request was submitted.
    model::term_id _term;
    // Set when the result for this request is finished. This is a shared
    // promise because a client can retry an already in progress request
    // (eg: timeouts) and we just chain the retried request request with
    // with the future from an already in-flight promise with the sequence
    // number match.
    result_promise_t result;

    bool in_progress() { return !result.available(); }
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
    using request_result_t
      = std::variant<errc, ss::future<result<kafka_result>>, request_ptr>;
    request_result_t
    new_request(seq_t first, seq_t last, model::term_id current);

    bool stm_apply(const model::batch_identity& bid, kafka::offset offset);

    void shutdown();

    friend std::ostream& operator<<(std::ostream&, const requests&);

private:
    static constexpr int32_t requests_cached_max = 5;
    std::optional<request_ptr> last_request() const;
    std::deque<request_ptr> _inflight_requests;
    std::deque<request_ptr> _finished_requests;
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

    requests::request_result_t
    enqueue_request(const model::batch_identity&, model::term_id current_term);
    void update(const model::batch_identity&, kafka::offset);

    std::optional<seq_t> last_sequence_number() const;

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

    void tickle() { _last_updated_ts = ss::lowres_system_clock::now(); }

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
