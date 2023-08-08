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

#pragma once

#include "cluster/producer_state.h"
#include "config/property.h"
#include "seastarx.h"

#include <seastar/core/sharded.hh>

namespace cluster {

class producer_state_manager
  : public ss::peering_sharded_service<producer_state_manager> {
public:
    explicit producer_state_manager(
      config::binding<uint64_t> max_producer_ids,
      std::chrono::milliseconds tx_expiration_ms)
      : _tx_expiration_ms(tx_expiration_ms)
      , _max_ids(std::move(max_producer_ids)) {}

    ss::future<> start();
    ss::future<> stop();

    void register_producer(producer_state& state);
    void deregister_proudcer(producer_state& state);

private:
    static constexpr std::chrono::seconds period{5};
    void evict_excess_producers();
    void do_evict_excess_producers();

    bool can_evict_producer(const producer_state&) const;

    size_t _num_producers = 0;
    std::chrono::milliseconds _tx_expiration_ms;
    config::binding<uint64_t> _max_ids;
    intrusive_list<producer_state, &producer_state::_hook> _lru_producers;
    ss::timer<ss::steady_clock_type> _reaper;
    ss::gate _gate;
};
} // namespace cluster
