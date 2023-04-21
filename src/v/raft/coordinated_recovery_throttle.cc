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

#include "raft/coordinated_recovery_throttle.h"

#include "raft/logger.h"

#include <seastar/util/defer.hh>

#include <chrono>

namespace raft {

coordinated_recovery_throttle::token_bucket::token_bucket(
  size_t initial_capacity)
  : _sem(initial_capacity, "recovery_throttle") {}

ss::future<> coordinated_recovery_throttle::token_bucket::throttle(
  size_t size, ss::abort_source& as) {
    _waiting_bytes += size;
    auto decrement = ss::defer([this, size] { _waiting_bytes -= size; });
    co_await _sem.wait(as, size);
    _admitted_bytes_since_last_reset += size;
}

void coordinated_recovery_throttle::token_bucket::reset_capacity(
  size_t new_capacity) {
    auto current = _sem.current();
    if (current == new_capacity) {
        // nothing to do.
    } else if (current > new_capacity) {
        _sem.consume(current - new_capacity);
    } else {
        _sem.signal(new_capacity - current);
    }
    _admitted_bytes_since_last_reset = 0;
    vlog(
      raftlog.debug,
      "Throttler bucket capacity reset to: {}, waiting bytes: {}",
      _sem.current(),
      _waiting_bytes);
}

coordinated_recovery_throttle::coordinated_recovery_throttle(
  config::binding<size_t> rate_binding)
  : _rate_binding(std::move(rate_binding))
  , _throttler(fair_rate_per_shard()) {
    if (ss::this_shard_id() == _coordinator_shard) {
        _coordinator.set_callback([this] {
            ssx::spawn_with_gate(_gate, [this] {
                return coordinate_tick().then(
                  [this] { arm_coordinator_timer(); });
            });
        });
    }
}

void coordinated_recovery_throttle::arm_coordinator_timer() {
    static constexpr clock_t::duration period = std::chrono::seconds{1};
    if (!_gate.is_closed()) {
        vassert(!_coordinator.armed(), "Invalid coordinator state: armed");
        _coordinator.arm(clock_t::now() + period);
    }
}

void coordinated_recovery_throttle::shutdown() { _throttler.shutdown(); }

ss::future<> coordinated_recovery_throttle::start() {
    vlog(raftlog.info, "Starting recovery throttle, rate: {}", _rate_binding());
    if (ss::this_shard_id() == _coordinator_shard) {
        arm_coordinator_timer();
    }
    co_return;
}

ss::future<> coordinated_recovery_throttle::stop() {
    vlog(raftlog.info, "Stopping recovery throttle");
    if (ss::this_shard_id() == _coordinator_shard) {
        co_await _gate.close();
    }
}

size_t coordinated_recovery_throttle::required_capacity() const {
    return _throttler.waiting_bytes() + _throttler.admitted_bytes();
}

ss::future<>
coordinated_recovery_throttle::reset_capacity_all_shards(size_t new_capacity) {
    co_await container().invoke_on_all(
      [new_capacity](coordinated_recovery_throttle& local) {
          local._throttler.reset_capacity(new_capacity);
      });
}

ss::future<> coordinated_recovery_throttle::reset_capacity_on_shard(
  ss::shard_id shard, size_t new_capacity) {
    co_await container().invoke_on(
      shard, [new_capacity](coordinated_recovery_throttle& local) {
          local._throttler.reset_capacity(new_capacity);
      });
}

ss::future<> coordinated_recovery_throttle::coordinate_tick() {
    try {
        co_await do_coordinate_tick();
    } catch (...) {
        vlog(
          raftlog.error,
          "Error coordinating recovery bandwidths: {}",
          std::current_exception());
    }
}

ss::future<> coordinated_recovery_throttle::do_coordinate_tick() {
    vassert(
      ss::this_shard_id() == _coordinator_shard,
      "Coordination on incorrect shard: {}",
      ss::this_shard_id());

    if (_gate.is_closed()) {
        co_return;
    }

    auto fair_shard_rate = fair_rate_per_shard();

    const auto capacity_requirements = co_await container().map(
      [](coordinated_recovery_throttle& crt) {
          return crt.required_capacity();
      });

    size_t total_bandwidth_unused = 0;
    size_t total_bandwidth_deficit = 0;

    for (auto req : capacity_requirements) {
        if (req <= fair_shard_rate) {
            total_bandwidth_unused += (fair_shard_rate - req);
        } else {
            total_bandwidth_deficit += (req - fair_shard_rate);
        }
    }

    vlog(
      raftlog.trace,
      "Coordination tick: unused bandwidth: {}, deficit bandwidth: {}",
      total_bandwidth_unused,
      total_bandwidth_deficit);

    if (total_bandwidth_deficit == 0 || total_bandwidth_unused == 0) {
        // Most common case.
        // All shards operating well under capacity or every shard is
        // operating at full capacity and no extra bandwidth to offer.
        co_return co_await reset_capacity_all_shards(fair_shard_rate);
    }

    // Here we try to distribute the unused bandwidth among shards with deficit.
    // in a weighted fashion proportional to the shard's share of deficit.
    for (auto shard : boost::irange(ss::smp::count)) {
        auto req = capacity_requirements.at(shard);
        auto rate = fair_shard_rate;
        if (req <= fair_shard_rate) {
            rate = std::min(req, fair_shard_rate);
        } else {
            auto shard_deficit = req - fair_shard_rate;
            auto deficit_share = shard_deficit * 1.0L / total_bandwidth_deficit;
            rate = fair_shard_rate
                   + static_cast<size_t>(
                     deficit_share * total_bandwidth_unused);
        }
        co_await reset_capacity_on_shard(shard, rate);
    }
}

} // namespace raft