/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/deps_impl.h"

#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "cluster_link/logger.h"
#include "cluster_link/replication/mux_remote_consumer.h"
#include "kafka/server/write_at_offset_stm.h"
#include "ssx/future-util.h"

static constexpr auto sync_timeout = 10s;

namespace cluster_link::replication {

remote_data_source_factory::remote_data_source_factory(
  std::unique_ptr<mux_remote_consumer> consumer)
  : _consumer(std::move(consumer)) {}

// to keep the unique_ptr<mux_remote_consumer> fwd declaration happy
remote_data_source_factory::~remote_data_source_factory() = default;

std::unique_ptr<data_source>
remote_data_source_factory::make_source(const ::model::ntp& ntp) {
    return std::make_unique<remote_partition_source>(ntp.tp, *_consumer);
}

ss::future<> remote_partition_source::start() {
    _consumer.add(_tp);
    return ss::now();
}

ss::future<> remote_partition_source::stop() noexcept {
    auto f = _gate.close();
    co_await _consumer.remove(_tp);
    co_await std::move(f);
}

ss::future<> remote_partition_source::reset(kafka::offset offset) {
    auto holder = _gate.hold();
    _consumer.reset(_tp, offset);
    return ss::now();
}

ss::future<data_source::data>
remote_partition_source::fetch_next(ss::abort_source& as) {
    auto holder = _gate.hold();
    auto [batches, units] = co_await _consumer.fetch(_tp, as);
    co_return data_source::data{
      .batches = std::move(batches), .units = std::move(units)};
}

std::unique_ptr<data_sink>
local_partition_data_sink_factory::make_sink(const ::model::ntp& ntp) {
    auto partition = _partition_manager.local().get(ntp);
    if (!partition) {
        throw std::runtime_error(
          fmt::format("Partition not found: {} on this shard", ntp));
    }
    return std::make_unique<local_partition_sink>(std::move(partition));
}

local_partition_sink::local_partition_sink(
  ss::lw_shared_ptr<cluster::partition> partition)
  : _partition(std::move(partition))
  , _stm(_partition->raft()->stm_manager()->get<kafka::write_at_offset_stm>()) {
    vassert(
      _stm,
      "write_at_offset_stm not attached to partition {}",
      _partition->ntp());
}
ss::future<> local_partition_sink::start() {
    auto holder = _gate.hold();
    auto sync_offset = co_await _stm->get_expected_last_offset(sync_timeout);
    if (sync_offset.has_error()) {
        throw std::runtime_error(
          fmt::format(
            "Failed to sync write_at_offset_stm for partition {}: {}",
            _partition->ntp(),
            sync_offset.error().message()));
    }
    _last_replicated_offset = sync_offset.value();
}

ss::future<> local_partition_sink::stop() noexcept { return _gate.close(); }

kafka::offset local_partition_sink::last_replicated_offset() const {
    vassert(_last_replicated_offset, "Sink has not been started");
    return _last_replicated_offset.value();
}

raft::replicate_stages local_partition_sink::replicate(
  chunked_vector<::model::record_batch> batches,
  ::model::timeout_clock::duration timeout,
  ss::abort_source& as) {
    _gate.check();
    vassert(_last_replicated_offset, "Sink has not been started");
    vassert(
      !batches.empty(),
      "Cannot replicate empty batch vector {}",
      _partition->ntp());
    chunked_vector<kafka::offset> expected_offsets;
    for (const auto& batch : batches) {
        expected_offsets.push_back(::model::offset_cast(batch.base_offset()));
    }
    auto new_last_replicated = ::model::offset_cast(
      batches.back().last_offset());
    vlog(
      cllog.trace,
      "[{}] Replicating batches in range [{} - {}], last_replicated: {}, "
      "new_last_replicated: {}",
      _partition->ntp(),
      batches.front().header(),
      batches.back().header(),
      _last_replicated_offset,
      new_last_replicated);
    auto stages = _stm->replicate(
      std::move(batches),
      std::move(expected_offsets),
      _last_replicated_offset,
      timeout,
      as);
    vassert(
      new_last_replicated > _last_replicated_offset,
      "[{}] Replicating offsets must be monotonically increasing last "
      "replicated: {}, attempting to replicate: {}",
      _partition->ntp(),
      _last_replicated_offset,
      new_last_replicated);
    _last_replicated_offset = new_last_replicated;
    return stages;
}

void local_partition_sink::notify_replicator_failure(::model::term_id term) {
    if (_gate.is_closed()) {
        return;
    }
    if (_partition->term() == term) {
        ssx::spawn_with_gate(_gate, [this, term] {
            return _partition->raft()->step_down(
              fmt::format("Unable to start replicator in term: {}", term));
        });
    }
}
} // namespace cluster_link::replication
