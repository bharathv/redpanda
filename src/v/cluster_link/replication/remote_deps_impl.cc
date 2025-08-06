/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/remote_deps_impl.h"

#include "cluster/partition.h"

namespace cluster_link::replication {
ss::future<> remote_partition_source::start() {
    // Implementation for starting the remote partition data source
    return ss::now();
}

ss::future<> remote_partition_source::stop() noexcept {
    // Implementation for stopping the remote partition data source
    return ss::now();
}

ss::future<> remote_partition_source::reset(kafka::offset) {
    // Implementation for resetting the remote partition data source
    return ss::now();
}

ss::future<data_source::data>
remote_partition_source::fetch_next(ss::abort_source&) {
    co_return data_source::data{};
}

ss::future<> local_partition_sink::start() {
    // Implementation for starting the local partition sink
    return ss::now();
}

ss::future<> local_partition_sink::stop() noexcept { return ss::now(); }

kafka::offset local_partition_sink::last_replicated_offset() const {
    // Implementation for getting the last replicated offset
    return kafka::offset{0};
}

raft::replicate_stages local_partition_sink::replicate(
  chunked_vector<::model::record_batch>,
  ::model::timeout_clock::duration,
  ss::abort_source&) {
    // Implementation for replicating data to the local partition
    return raft::replicate_stages{raft::errc::success};
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
