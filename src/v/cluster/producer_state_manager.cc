/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "producer_state_manager.h"

#include "cluster/logger.h"

#include <seastar/util/defer.hh>

namespace cluster {
ss::future<> producer_state_manager::start() {
    _reaper.set_callback([this] { evict_excess_producers(); });
    _reaper.arm(ss::steady_clock_type::now());
    return ss::make_ready_future();
}

ss::future<> producer_state_manager::stop() {
    _reaper.cancel();
    return _gate.close();
}

void producer_state_manager::regist3r(producer_state& state) {
    link(state);
    ++_num_producers;
    vlog(clusterlog.debug, "Registered producer: {}", state._id);
}

void producer_state_manager::deregist3r(producer_state& state) {
    state._hook.unlink();
    --_num_producers;
    vlog(clusterlog.debug, "Removing producer: {}", state._id);
}

void producer_state_manager::link(producer_state& state) {
    vassert(
      !state._hook.is_linked(),
      "double linking of producer state {}",
      state._id);
    _lru_producers.push_back(state);
}

bool producer_state_manager::can_evict_producer(
  const producer_state& state) const {
    return _num_producers > _max_ids()
           || std::chrono::duration_cast<std::chrono::milliseconds>(
                ss::lowres_system_clock::now() - state._last_updated_ts)
                > _tx_expiration_ms;
}

void producer_state_manager::evict_excess_producers() {
    ssx::background = ssx::spawn_with_gate_then(_gate, [this]() {
                          do_evict_excess_producers();
                      }).finally([this] {
        if (!_gate.is_closed()) {
            _reaper.arm(period);
        }
    });
}

void producer_state_manager::do_evict_excess_producers() {
    if (_gate.is_closed()) {
        return;
    }
    auto it = _lru_producers.begin();
    while (it != _lru_producers.end() && can_evict_producer(*it)) {
        auto it_copy = it;
        auto& state = *it_copy;
        state.evict(); // schedules a cleanup.
        --_num_producers;
        ++it;
    }
}

}; // namespace cluster
