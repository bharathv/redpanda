/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

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

#include "cluster/tests/cluster_test_fixture.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/coordinator/state_machine.h"

static ss::logger logger{"datalake-test-logger"};
namespace datalake::tests {

class datalake_cluster_test_fixture : public cluster_test_fixture {
public:
    fixture_ptr make_redpanda_fixture(
      model::node_id node_id,
      int16_t kafka_port,
      int16_t rpc_port,
      int16_t proxy_port,
      int16_t schema_reg_port,
      std::vector<config::seed_server> seeds,
      configure_node_id use_node_id,
      empty_seed_starts_cluster empty_seed_starts_cluster_val,
      std::optional<cloud_storage_clients::s3_configuration> s3_config
      = std::nullopt,
      std::optional<archival::configuration> archival_cfg = std::nullopt,
      std::optional<cloud_storage::configuration> cloud_cfg = std::nullopt,
      bool enable_legacy_upload_mode = false) override {
        return std::make_unique<redpanda_thread_fixture>(
          node_id,
          kafka_port,
          rpc_port,
          proxy_port,
          schema_reg_port,
          seeds,
          ssx::sformat("{}.{}", _base_dir, node_id()),
          _sgroups,
          false,
          s3_config,
          archival_cfg,
          cloud_cfg,
          use_node_id,
          empty_seed_starts_cluster_val,
          std::nullopt,
          false,
          enable_legacy_upload_mode,
          /* iceberg */ true);
    }

    ss::future<> create_iceberg_topic(
      model::topic topic, int num_partitions = 1, int16_t num_replicas = 3) {
        cluster::topic_properties props;
        props.iceberg_enabled = true;
        props.iceberg_translation_interval_ms = 50ms;
        return create_topic(
          {model::kafka_namespace, topic},
          num_partitions,
          num_replicas,
          std::move(props));
    }

    ss::sharded<datalake::coordinator::frontend>&
    coordinator_frontend(model::node_id id) {
        return instance(id)->app.datalake_coordinator_frontend();
    }

    ss::future<unchecked<
      chunked_vector<datalake::coordinator::translated_offset_range>,
      datalake::coordinator_errc>>
    translated_files_for_partition(const model::ntp& ntp) {
        chunked_vector<datalake::coordinator::translated_offset_range> result;
        auto& fe = coordinator_frontend(model::node_id{0});
        auto coordinator_partition = fe.local().coordinator_partition(ntp.tp);
        if (!coordinator_partition) {
            co_return datalake::coordinator_errc::not_leader;
        }
        auto c_ntp = model::ntp{
          model::datalake_coordinator_nt.ns,
          model::datalake_coordinator_nt.tp,
          coordinator_partition.value()};
        auto [_, partition] = get_leader(c_ntp);
        if (!partition) {
            co_return datalake::coordinator_errc::not_leader;
        }
        auto stm = partition->raft()
                     ->stm_manager()
                     ->get<datalake::coordinator::coordinator_stm>();
        if (!stm) {
            co_return datalake::coordinator_errc::not_leader;
        }
        const auto& state = stm->state().topic_to_state;
        auto it = state.find(ntp.tp.topic);
        if (
          it == state.end()
          || !it->second.pid_to_pending_files.contains(ntp.tp.partition)) {
            co_return result;
        }
        for (auto& range : it->second.pid_to_pending_files.at(ntp.tp.partition)
                             .pending_entries) {
            result.push_back(range.copy());
        }
        co_return result;
    }

    ss::future<> validate_translated_files(const model::ntp& ntp) {
        // Wait until all all the data is translated.
        auto [fixture, partition] = get_leader(ntp);
        if (!partition) {
            throw std::runtime_error("leader not found during validation");
        }
        auto max_offset = model::offset_cast(
          partition->get_offset_translator_state()->from_log_offset(
            partition->dirty_offset()));
        auto& fe = coordinator_frontend(fixture->app.controller->self());
        coordinator::fetch_latest_data_file_request request;
        request.tp = ntp.tp;
        co_await ::tests::cooperative_spin_wait_with_timeout(20s, [&] {
            return fe.local().fetch_latest_data_file(request).then(
              [max_offset](coordinator::fetch_latest_data_file_reply resp) {
                  return resp.last_added_offset
                         && resp.last_added_offset.value() >= max_offset;
              });
        });
        // validate the continuity of the offset space.
        auto files = co_await translated_files_for_partition(ntp);
        if (files.has_error() || files.value().empty()) {
            throw std::runtime_error("No translated files found");
        }
        vlog(logger.info, "Translated files: {}", files.value());
        std::optional<kafka::offset> last_end;
        for (auto& f : files.value()) {
            BOOST_REQUIRE(
              !last_end
              || f.last_offset == kafka::next_offset(last_end.value()));
            last_end = f.last_offset;
        }
    }
};

} // namespace datalake::tests
