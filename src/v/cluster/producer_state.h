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

namespace cluster {

class producer_state_manager;

class producer_state {
public:
    producer_state(
      producer_state_manager& mgr,
      model::producer_identity id,
      raft::group_id group,
      model::term_id term,
      ss::noncopyable_function<void()> hook)
      : _id(id)
      , _group(group)
      , _term(term)
      , _parent(std::ref(mgr))
      , _last_updated_ts(ss::lowres_system_clock::now())
      , _eviction_hook(std::move(hook)) {
        register_self();
        _requests.reserve(requests_cached_max);
    }

    producer_state(const producer_state&) = delete;
    producer_state& operator=(producer_state&) = delete;
    producer_state(producer_state&&) noexcept;
    producer_state& operator=(producer_state&& other) noexcept;
    ~producer_state() noexcept = default;

    template<class AsyncFunc>
    auto run_func(AsyncFunc&& func) {
        return ss::with_gate(
          _gate, [this, func = std::forward<AsyncFunc>(func)]() mutable {
              return _op_lock.get_units().then(
                [this,
                 func = std::forward<AsyncFunc>(func)](auto units) mutable {
                    deregister_self(); // avoids eviction as long as the op is
                                       // in progress
                    return ss::futurize_invoke(
                             std::forward<AsyncFunc>(func), std::move(units))
                      .then_wrapped([this](auto result) mutable {
                          register_self(); // enqueue back at the end to
                                           // preserve LRU-ness
                          return result;
                      });
                });
          });
    }

    ss::future<> shutdown();

    using shared_promise_t = ss::shared_promise<result<kafka_result>>;
    struct request {
        request(int32_t first, int32_t last, shared_promise_t res)
          : first_sequence(first)
          , last_sequence(last)
          , result(std::move(res)) {}

        int32_t first_sequence;
        int32_t last_sequence;
        shared_promise_t result;

        bool in_progress() { return !result.available(); }
    };
    using request_ptr = ss::lw_shared_ptr<request>;
    using enqueue_result
      = std::variant<errc, ss::future<result<kafka_result>>, request_ptr>;
    enqueue_result
    enqueue_request(const model::batch_identity&, model::term_id current_term);
    void update(const model::batch_identity&, kafka::offset);
    void evict() {
        if (!_evicted) {
            _eviction_hook();
            _evicted = true;
        }
    }

private:
    static constexpr int32_t requests_cached_max = 5;

    void register_self();
    void deregister_self();

    void tickle() { _last_updated_ts = ss::lowres_system_clock::now(); }

    model::producer_identity _id;
    raft::group_id _group;
    model::term_id _term;
    mutex _op_lock;
    std::reference_wrapper<producer_state_manager> _parent;
    // invariant: all entries in _request_queue are ordered by sequence_number
    // this is checked before appending a new request to the queue.
    // requests are submitted in the order of sequence numbers.
    // If a sequence N is finished, sequence N-1 is guaranteed to be finished.
    // We retain a maximum of `requests_cached_max` finished requests. Those
    // the leading elements in the request queue. Kafka clients only issue
    // requests in batches of 5, the queue is fairly small at all times.
    // [<5 finished> , <5 inflight>]. finished requests exceeding cache
    // limit are gc-ed in update()
    ss::circular_buffer<request_ptr> _requests;
    ss::lowres_system_clock::time_point _last_updated_ts;
    intrusive_list_hook _hook;
    ss::gate _gate;
    // function hook called on eviction
    bool _evicted = false;
    ss::noncopyable_function<void()> _eviction_hook;
    friend class producer_state_manager;
};
} // namespace cluster
