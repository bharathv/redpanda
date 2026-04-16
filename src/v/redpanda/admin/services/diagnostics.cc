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

#include "redpanda/admin/services/diagnostics.h"

#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin {

diagnostics_service_impl::diagnostics_service_impl(
  ss::sharded<diagnostics::event_buffer>& event_buffer)
  : _event_buffer(event_buffer) {}

ss::future<proto::admin::get_event_log_response>
diagnostics_service_impl::get_event_log(
  serde::pb::rpc::context, proto::admin::get_event_log_request req) {
    diagnostics::event_filter filter;

    if (req.has_since_seconds()) {
        filter.since = diagnostics::diagnostic_event::clock_type::now()
                       - std::chrono::seconds(req.get_since_seconds());
    }
    if (req.has_min_severity()) {
        filter.min_severity = static_cast<diagnostics::severity>(
          req.get_min_severity());
    }
    if (req.has_subsystem_filter()) {
        filter.subsystem_filter = static_cast<diagnostics::subsystem>(
          req.get_subsystem_filter());
    }

    proto::admin::get_event_log_response resp;

    // Gather events from all shards. Each shard returns its own vector,
    // then we merge on the local shard. Cannot write to resp from remote
    // shards (cross-shard data race).
    auto per_shard_events = co_await _event_buffer.map(
      [&filter](diagnostics::event_buffer& buf) { return buf.query(filter); });

    for (auto& shard_events : per_shard_events) {
        for (const auto& ev : shard_events) {
            resp.get_events().push_back(to_proto(ev));
        }
    }

    resp.set_buffer_capacity(
      static_cast<int32_t>(_event_buffer.local().capacity()));
    resp.set_total_events_stored(
      static_cast<int32_t>(_event_buffer.local().size()));

    co_return resp;
}

ss::future<proto::admin::get_diagnostics_response>
diagnostics_service_impl::get_diagnostics(
  serde::pb::rpc::context, proto::admin::get_diagnostics_request) {
    // TODO: wire up real metrics from storage, raft, and resource monitors.
    proto::admin::get_diagnostics_response resp;
    co_return resp;
}

proto::admin::diagnostic_event diagnostics_service_impl::to_proto(
  const diagnostics::diagnostic_event& ev) const {
    proto::admin::diagnostic_event out;

    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      ev.timestamp.time_since_epoch());
    out.set_timestamp_ns(ns.count());
    out.set_shard_id(ev.shard_id);
    out.set_severity(static_cast<proto::admin::severity>(ev.severity));
    out.set_subsystem(static_cast<proto::admin::subsystem>(ev.subsystem));

    if (ev.partition) {
        proto::admin::partition p;
        p.set_namespace(ss::sstring(ev.partition->ns()));
        p.set_topic(ss::sstring(ev.partition->tp.topic()));
        p.set_id(ev.partition->tp.partition());
        out.set_partition(std::move(p));
    }

    std::visit(
      [&out](const auto& payload) {
          using T = std::decay_t<decltype(payload)>;

          if constexpr (
            std::is_same_v<T, diagnostics::leadership_transfer_event>) {
              proto::admin::leadership_transfer_event e;
              e.set_from_broker(payload.from_broker());
              e.set_to_broker(payload.to_broker());
              e.set_reason(
                static_cast<proto::admin::leadership_transfer_reason>(
                  payload.reason));
              out.set_leadership_transfer(std::move(e));

          } else if constexpr (
            std::is_same_v<T, diagnostics::partition_move_event>) {
              proto::admin::partition_move_event e;
              e.set_phase(
                static_cast<proto::admin::partition_move_phase>(payload.phase));
              e.set_source_broker(payload.source_broker());
              e.set_target_broker(payload.target_broker());
              e.set_total_size_bytes(payload.total_size_bytes);
              e.set_progress_pct(payload.progress_pct);
              e.set_bytes_moved(payload.bytes_moved);
              e.set_stall_reason(
                static_cast<proto::admin::partition_move_stall_reason>(
                  payload.stall_reason));
              out.set_partition_move(std::move(e));

          } else if constexpr (
            std::is_same_v<T, diagnostics::resource_pressure_event>) {
              proto::admin::resource_pressure_event e;
              e.set_resource(
                static_cast<proto::admin::resource_type>(payload.resource));
              e.set_usage_pct(payload.usage_pct);
              e.set_threshold_pct(payload.threshold_pct);
              e.set_impacted_subsystem(
                static_cast<proto::admin::impacted_subsystem>(
                  payload.impacted));
              if (payload.latency_p99_ms) {
                  e.set_latency_p99_ms(*payload.latency_p99_ms);
              }
              out.set_resource_pressure(std::move(e));

          } else if constexpr (
            std::is_same_v<T, diagnostics::node_lifecycle_event>) {
              proto::admin::node_lifecycle_event e;
              e.set_broker_id(payload.broker_id());
              e.set_phase(
                static_cast<proto::admin::node_lifecycle_phase>(payload.phase));
              e.set_reason(
                static_cast<proto::admin::node_lifecycle_reason>(
                  payload.reason));
              out.set_node_lifecycle(std::move(e));

          } else if constexpr (
            std::is_same_v<T, diagnostics::replica_set_change_event>) {
              proto::admin::replica_set_change_event e;
              e.set_type(
                static_cast<proto::admin::replica_set_change_type>(
                  payload.type));
              for (auto id : payload.previous_replicas) {
                  e.get_previous_replicas().push_back(id());
              }
              for (auto id : payload.current_replicas) {
                  e.get_current_replicas().push_back(id());
              }
              e.set_reason(
                static_cast<proto::admin::replica_set_change_reason>(
                  payload.reason));
              out.set_replica_set_change(std::move(e));

          } else if constexpr (
            std::is_same_v<T, diagnostics::config_change_event>) {
              proto::admin::config_change_event e;
              e.set_key(ss::sstring(payload.key));
              e.set_old_value(ss::sstring(payload.old_value));
              e.set_new_value(ss::sstring(payload.new_value));
              e.set_source(
                static_cast<proto::admin::config_change_source>(
                  payload.source));
              out.set_config_change(std::move(e));

          } else if constexpr (std::is_same_v<T, diagnostics::error_event>) {
              proto::admin::error_event e;
              std::visit(
                [&e](const auto& err_payload) {
                    using E = std::decay_t<decltype(err_payload)>;
                    if constexpr (
                      std::is_same_v<E, diagnostics::generic_error>) {
                        proto::admin::generic_error ge;
                        ge.set_message(ss::sstring(err_payload.message));
                        e.set_generic(std::move(ge));
                    }
                    // TODO: map remaining known error types
                },
                payload.payload);
              out.set_error(std::move(e));
          }
      },
      ev.payload);

    return out;
}

} // namespace admin
