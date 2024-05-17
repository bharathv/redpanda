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

#include "cluster/namespaced_cache.h"
#include "cluster/rm_stm_types.h"
#include "cluster/types.h"
#include "container/intrusive_list_helpers.h"
#include "model/record.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <bit>
#include <chrono>

using namespace std::chrono_literals;

// Befriended to expose internal state in tests.
struct test_fixture;

namespace cluster::tx {

template<class Func>
concept AcceptsUnits = requires(Func f, ssx::semaphore_units units) {
    f(std::move(units));
};

using producer_ptr = ss::lw_shared_ptr<producer_state>;
using result_promise_t = ss::shared_promise<result<kafka_result>>;
using request_ptr = ss::lw_shared_ptr<request>;
using seq_t = int32_t;

enum class request_state : uint8_t {
    initialized = 0,
    in_progress = 1,
    completed = 2
};

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
            _state = request_state::completed;
        }
    }

    template<class ValueType>
    void set_value(ValueType&& value) {
        vassert(
          _state <= request_state::in_progress && !_result.available(),
          "unexpected request state during set: state: {}, result available: "
          "{}",
          static_cast<std::underlying_type_t<request_state>>(_state),
          _result.available());
        _result.set_value(std::forward<ValueType>(value));
        _state = request_state::completed;
    }
    void mark_request_in_progress() { _state = request_state::in_progress; }
    request_state state() const { return _state; }
    result_promise_t::future_type result() const;

    bool operator==(const request&) const;

private:
    request_state _state{request_state::initialized};
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

    bool has_completed() { return _state == request_state::completed; }
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

    void stm_apply(
      const model::batch_identity& bid, model::term_id, kafka::offset offset);

    void shutdown();

    bool operator==(const requests&) const;
    friend std::ostream& operator<<(std::ostream&, const requests&);

private:
    static constexpr int32_t requests_cached_max = 5;
    // chunk size of the request containers to avoid wastage.
    static constexpr size_t chunk_size = std::bit_ceil(
      static_cast<unsigned long>(requests_cached_max));
    bool is_valid_sequence(seq_t incoming) const;
    std::optional<request_ptr> last_request() const;
    void gc_requests_from_older_terms(model::term_id current);
    ss::chunked_fifo<request_ptr, chunk_size> _inflight_requests;
    ss::chunked_fifo<request_ptr, chunk_size> _finished_requests;
    friend producer_state;
};

/// Encapsulates all the state of a producer producing batches to
/// a single raft group.
class producer_state {
public:
    using clock_type = ss::lowres_system_clock;

    producer_state(
      prefix_logger& logger,
      model::producer_identity id,
      raft::group_id group,
      ss::noncopyable_function<void()> post_eviction_hook)
      : _logger(logger)
      , _id(id)
      , _group(group)
      , _last_updated_ts(ss::lowres_system_clock::now())
      , _post_eviction_hook(std::move(post_eviction_hook)) {}
    producer_state(
      prefix_logger&,
      ss::noncopyable_function<void()> post_eviction_hook,
      producer_state_snapshot) noexcept;

    producer_state(const producer_state&) = delete;
    producer_state& operator=(producer_state&) = delete;
    producer_state(producer_state&&) noexcept = delete;
    producer_state& operator=(producer_state&& other) noexcept = delete;
    ~producer_state() noexcept = default;
    bool operator==(const producer_state& other) const;

    friend std::ostream& operator<<(std::ostream& o, const producer_state&);

    /// Runs the passed async function under the op_lock scope.

    template<AcceptsUnits AsyncFunc>
    auto run_with_lock(AsyncFunc&& func) {
        if (_evicted) {
            throw ss::gate_closed_exception();
        }
        return _op_lock.get_units().then(
          [f = std::forward<AsyncFunc>(func)](auto units) {
              return f(std::move(units));
          });
    }

    void shutdown_input();
    bool can_evict();
    bool is_evicted() const { return _evicted; }

    /* reset sequences resets the tracking state and skips the sequence
     * checks.*/
    result<request_ptr> try_emplace_request(
      const model::batch_identity&,
      model::term_id current_term,
      bool reset_sequences = false);

    void apply_data(const model::record_batch_header&, kafka::offset);

    void apply_transaction_begin(
      const model::record_batch_header&, const fence_batch_data& parsed_batch);

    std::optional<model::tx_range>
      apply_transaction_end(model::control_record_type);

    void touch() { _last_updated_ts = ss::lowres_system_clock::now(); }

    std::optional<seq_t> last_sequence_number() const;

    producer_state_snapshot snapshot(kafka::offset log_start_offset) const;

    ss::lowres_system_clock::time_point get_last_update_timestamp() const {
        return _last_updated_ts;
    }

    model::timestamp last_update_timestamp() const {
        return model::timestamp(_last_updated_ts.time_since_epoch() / 1ms);
    }

    std::optional<kafka::offset> current_txn_start_offset() const {
        return _current_txn_start_offset;
    }

    model::producer_identity id() const { return _id; }

    void update_current_txn_start_offset(std::optional<kafka::offset> offset) {
        _current_txn_start_offset = offset;
    }

    void gc_requests_from_older_terms(model::term_id current_term) {
        _requests.gc_requests_from_older_terms(current_term);
    }

    bool has_transaction_in_progress() const {
        return bool(_transaction_state);
    }

    const std::optional<producer_partition_transaction_state>&
    transaction_state() const {
        return _transaction_state;
    }

    // Returns true if there is an open transaction _and_ if it
    // has expired.
    bool has_transaction_expired() const;

    void force_transaction_expiry() { _force_transaction_expiry = true; }

    // Used to track all active producers on a shard (across all the
    // partitions).
    safe_intrusive_list_hook _hook;

    // Used to track all the active transactions in a partition.
    // The hook is linked (in the state machine) if there is an open transaction
    // on the partition using this producer.
    safe_intrusive_list_hook _active_transaction_hook;

private:
    std::chrono::milliseconds ms_since_last_update() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
          ss::lowres_system_clock::now() - _last_updated_ts);
    }

    prefix_logger& _logger;

    // --- Following state is serialized to snapshot
    //
    model::producer_identity _id;
    raft::group_id _group;
    requests _requests;
    // Tracks the last time an operation is run with this producer.
    // Used to evict stale producers.
    ss::lowres_system_clock::time_point _last_updated_ts;
    // Disengaged optional indicates no in progress transaction for
    // this producer.
    std::optional<producer_partition_transaction_state> _transaction_state;

    // --- In memory state below, not serialized to snapshot
    //
    // serializes all the operations on this producer
    mutex _op_lock{"producer_state::_op_lock"};
    // Used to implement force eviction via admin APIs
    bool _force_transaction_expiry{false};
    bool _evicted = false;
    ss::noncopyable_function<void()> _post_eviction_hook;
    std::optional<kafka::offset> _current_txn_start_offset;
    friend class producer_state_manager;
    friend struct ::test_fixture;
};

} // namespace cluster::tx
