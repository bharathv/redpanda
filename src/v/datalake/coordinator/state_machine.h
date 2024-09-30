/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "cluster/state_machine_registry.h"
#include "container/chunked_hash_map.h"
#include "datalake/coordinator/types.h"
#include "raft/persisted_stm.h"

namespace datalake::coordinator {

class coordinator_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "datalake_coordinator_stm";

    explicit coordinator_stm(ss::logger&, raft::consensus*);

    ss::future<> do_apply(const model::record_batch&) override;

    model::offset max_collectible_offset() override;

    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&& bytes) override;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) override;

    ss::future<> apply_raft_snapshot(const iobuf&) final;

    ss::future<iobuf> take_snapshot(model::offset) final;

    ss::future<add_translated_data_files_reply>
      add_translated_data_file(add_translated_data_files_request);

    ss::future<fetch_latest_data_file_reply>
      fetch_latest_data_file(fetch_latest_data_file_request);

private:
    using translated_files = chunked_vector<translated_data_file_entry>;
    struct partition_file_info {
        translated_files files;
        bool request_in_progress;
    };
    using partition_to_files
      = chunked_hash_map<model::partition_id, partition_file_info>;
    using topic_uncommitted_files
      = chunked_hash_map<model::topic, partition_to_files>;

    topic_uncommitted_files _all_files;
};
class stm_factory : public cluster::state_machine_factory {
public:
    stm_factory() = default;
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(raft::state_machine_manager_builder&, raft::consensus*) final;
};
} // namespace datalake::coordinator
