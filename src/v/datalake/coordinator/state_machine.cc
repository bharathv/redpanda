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

#include "datalake/coordinator/state_machine.h"

#include "datalake/logger.h"

namespace datalake::coordinator {

coordinator_stm::coordinator_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>("datalake_coordinator_stm.snapshot", logger, raft) {}

ss::future<> coordinator_stm::do_apply(const ::model::record_batch&) {
    co_return;
}

::model::offset coordinator_stm::max_collectible_offset() { return {}; }

ss::future<>
coordinator_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) {
    co_return;
}

ss::future<raft::stm_snapshot>
coordinator_stm::take_local_snapshot(ssx::semaphore_units) {
    return ss::make_exception_future<raft::stm_snapshot>(
      std::runtime_error{"not implemented exception"});
}

ss::future<> coordinator_stm::apply_raft_snapshot(const iobuf&) { co_return; }

ss::future<iobuf> coordinator_stm::take_snapshot(::model::offset) {
    co_return iobuf{};
}

ss::future<add_translated_data_file_reply>
coordinator_stm::add_translated_data_file(
  add_translated_data_files_request req) {
    if (_raft->is_leader()) {
        co_return add_translated_data_file_reply{coordinator_errc::not_leader};
    }
    // todo: replicate first, this is just for demonstration.
    auto [it, _] = _data_files.try_emplace(req.topic_partition());
    for (auto& entry : req.files) {
        it->second.push_back(std::move(entry));
    }
    co_return add_translated_data_file_reply{coordinator_errc::ok};
}

ss::future<fetch_latest_data_file_reply>
coordinator_stm::fetch_latest_data_file(
  fetch_latest_data_file_request request) {
    if (_raft->is_leader()) {
        co_return fetch_latest_data_file_reply{coordinator_errc::not_leader};
    }
    fetch_latest_data_file_reply reply;
    auto it = _data_files.find(request.topic_partition());
    if (it != _data_files.end() && !it->second.empty()) {
        reply.entry = it->second.back();
    }
    co_return reply;
}

bool stm_factory::is_applicable_for(const storage::ntp_config& config) const {
    const auto& ntp = config.ntp();
    return (ntp.ns == ::model::datalake_coordinator_nt.ns)
           && (ntp.tp.topic == ::model::datalake_coordinator_topic);
}

void stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto stm = builder.create_stm<coordinator_stm>(datalake_log, raft);
    raft->log()->stm_manager()->add_stm(stm);
}

} // namespace datalake::coordinator
