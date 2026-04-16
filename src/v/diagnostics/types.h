/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"

#include <seastar/core/sstring.hh>

#include <chrono>
#include <optional>
#include <variant>
#include <vector>

namespace diagnostics {

// Enum values start at 1 to match proto where 0 is *_UNSPECIFIED.

enum class severity : uint8_t {
    info = 1,
    warn = 2,
    error = 3,
};

enum class subsystem : uint8_t {
    raft = 1,
    partition_manager = 2,
    storage = 3,
    node_lifecycle = 4,
    resource_monitor = 5,
    config = 6,
    rpc = 7,
};

// --- Operational event payloads ---

enum class leadership_transfer_reason : uint8_t {
    preference = 1,
    node_down = 2,
    decommission = 3,
    maintenance = 4,
    manual = 5,
};

struct leadership_transfer_event {
    model::node_id from_broker;
    model::node_id to_broker;
    leadership_transfer_reason reason;
};

enum class partition_move_phase : uint8_t {
    start = 1,
    progress = 2,
    stall = 3,
    complete = 4,
    cancel = 5,
};

enum class partition_move_stall_reason : uint8_t {
    unspecified = 0,
    target_disk_full = 1,
    target_disk_throttled = 2,
    source_unavailable = 3,
    network_saturated = 4,
    memory_pressure = 5,
    cross_rack_bandwidth = 6,
    learner_recovery_slow = 7,
};

struct partition_move_event {
    partition_move_phase phase;
    model::node_id source_broker;
    model::node_id target_broker;
    int64_t total_size_bytes{0};
    float progress_pct{0.0f};
    int64_t bytes_moved{0};
    partition_move_stall_reason stall_reason{
      partition_move_stall_reason::unspecified};
};

enum class resource_type : uint8_t {
    disk = 1,
    memory = 2,
    cpu = 3,
    network = 4,
};

enum class impacted_subsystem : uint8_t {
    unspecified = 0,
    log_storage = 1,
    raft_recovery = 2,
    compaction = 3,
    fetch = 4,
    produce = 5,
    tiered_storage = 6,
};

struct resource_pressure_event {
    resource_type resource;
    float usage_pct;
    float threshold_pct;
    impacted_subsystem impacted;
    std::optional<float> latency_p99_ms;
};

enum class node_lifecycle_phase : uint8_t {
    join = 1,
    decommission_start = 2,
    decommission_complete = 3,
    maintenance_enter = 4,
    maintenance_exit = 5,
    down_detected = 6,
    recovery = 7,
};

enum class node_lifecycle_reason : uint8_t {
    unspecified = 0,
    operator_initiated = 1,
    health_check_failure = 2,
    rpc_timeout = 3,
    upgrade = 4,
    scaling = 5,
};

struct node_lifecycle_event {
    model::node_id broker_id;
    node_lifecycle_phase phase;
    node_lifecycle_reason reason{node_lifecycle_reason::unspecified};
};

enum class replica_set_change_type : uint8_t {
    shrink = 1,
    expand = 2,
    reconfigure = 3,
};

enum class replica_set_change_reason : uint8_t {
    unspecified = 0,
    rebalance = 1,
    decommission = 2,
    node_down = 3,
    rack_constraint = 4,
    manual = 5,
};

struct replica_set_change_event {
    replica_set_change_type type;
    std::vector<model::node_id> previous_replicas;
    std::vector<model::node_id> current_replicas;
    replica_set_change_reason reason{replica_set_change_reason::unspecified};
};

enum class config_change_source : uint8_t {
    api = 1,
    startup = 2,
    default_change = 3,
};

struct config_change_event {
    ss::sstring key;
    ss::sstring old_value;
    ss::sstring new_value;
    config_change_source source;
};

// --- Error event payloads ---

struct oom_event {
    float memory_usage_pct;
    int64_t memory_available_bytes;
    subsystem requesting_subsystem;
};

struct segment_corruption_event {
    ss::sstring segment_path;
    int64_t offset;
};

struct rpc_timeout_cascade_event {
    model::node_id target_broker;
    int32_t timeout_count;
    float latency_p99_ms;
};

enum class disk_failure_type : uint8_t {
    io_error = 1,
    read_only = 2,
    space_exhausted = 3,
};

struct disk_failure_event {
    ss::sstring mount_point;
    disk_failure_type failure_type;
};

enum class allocation_failure_reason : uint8_t {
    insufficient_nodes = 1,
    disk_full = 2,
    rack_constraint = 3,
};

struct allocation_failure_event {
    int32_t requested_replicas;
    int32_t available_nodes;
    allocation_failure_reason reason;
};

struct raft_recovery_failure_event {
    model::node_id target_broker;
    int64_t log_offset_gap;
};

struct generic_error {
    ss::sstring message;
};

using error_payload = std::variant<
  oom_event,
  segment_corruption_event,
  rpc_timeout_cascade_event,
  disk_failure_event,
  allocation_failure_event,
  raft_recovery_failure_event,
  generic_error>;

struct error_event {
    error_payload payload;
};

// --- Envelope ---

using event_payload = std::variant<
  leadership_transfer_event,
  partition_move_event,
  resource_pressure_event,
  node_lifecycle_event,
  replica_set_change_event,
  config_change_event,
  error_event>;

struct diagnostic_event {
    using clock_type = std::chrono::system_clock;

    clock_type::time_point timestamp;
    unsigned shard_id;
    std::optional<model::ntp> partition;
    severity severity;
    subsystem subsystem;
    event_payload payload;
};

} // namespace diagnostics
