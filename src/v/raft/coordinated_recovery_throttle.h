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

#include "config/configuration.h"
#include "seastarx.h"
#include "utils/token_bucket.h"

#include <seastar/core/sharded.hh>

namespace raft {

/// A recovery throttle that coordinates the total available rate across shards.
/// Coordination is done by shard 0. The intent of the coordination is to
/// saturate the available node recovery rate (bandwidth). This is achieved by
/// redistributing the unused bandwidth from idle shards among the busy shards.
/// Internally it wraps a simple token bucket (semaphore) that is periodically
/// reset with the desired capacity as deemed by the coordinator.

/// In order to guarantee fairness, each shard always has access to its fair
/// share of bandwidth (= total_available/num_shards). Any unused portion of
/// this fair share is redistributed among the busy shards.
class coordinated_recovery_throttle
  : public ss::peering_sharded_service<coordinated_recovery_throttle> {
public:
    explicit coordinated_recovery_throttle(config::binding<size_t>);
    coordinated_recovery_throttle(const coordinated_recovery_throttle&)
      = delete;
    coordinated_recovery_throttle&
    operator=(const coordinated_recovery_throttle&)
      = delete;
    coordinated_recovery_throttle(coordinated_recovery_throttle&&) = delete;
    coordinated_recovery_throttle& operator=(coordinated_recovery_throttle&&)
      = delete;
    ~coordinated_recovery_throttle() noexcept = default;

    void shutdown();

    ss::future<> start();
    ss::future<> stop();

    ss::future<> tick_for_testing() { return do_coordinate_tick(); }

    ss::future<> throttle(size_t size, ss::abort_source& as) {
        return _throttler.throttle(size, as);
    }

    size_t available() const { return _throttler.available(); }
    size_t waiting_bytes() const { return _throttler.waiting_bytes(); }
    size_t admitted_bytes() const { return _throttler.admitted_bytes(); }

private:
    using clock_t = ss::lowres_clock;
    static constexpr ss::shard_id _coordinator_shard = ss::shard_id{0};

    /// This is a glorified semaphore that tracks the total throttled
    /// bytes due to non availability of capacity. The capacity of
    /// of this bucket is managed by the recovery throttle.
    class token_bucket {
    public:
        explicit token_bucket(size_t /*initial_size*/);
        ss::future<> throttle(size_t /*bytes*/, ss::abort_source&);
        void reset_capacity(size_t /*new capacity*/);
        void shutdown() { return _sem.broken(); }
        inline size_t waiting_bytes() const { return _waiting_bytes; }
        inline size_t admitted_bytes() const {
            return _admitted_bytes_since_last_reset;
        }
        inline size_t available() const { return _sem.current(); }

    private:
        ssx::named_semaphore<> _sem;
        size_t _waiting_bytes{0};
        size_t _admitted_bytes_since_last_reset{0};
    };

    inline size_t fair_rate_per_shard() const {
        return _rate_binding() / ss::smp::count;
    }

    ss::future<> reset_capacity_all_shards(size_t /* new capacity*/);
    ss::future<> reset_capacity_on_shard(ss::shard_id, size_t /*new capacity*/);

    void arm_coordinator_timer();

    size_t required_capacity() const;

    ss::future<> coordinate_tick();
    ss::future<> do_coordinate_tick();

    config::binding<size_t> _rate_binding;
    token_bucket _throttler;
    ss::gate _gate;
    ss::timer<clock_t> _coordinator;
};

} // namespace raft