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

#include <seastar/util/defer.hh>

namespace {

static constexpr std::chrono::milliseconds wait_timeout{5000};

raft::replicate_options make_replicate_options() {
    auto opts = raft::replicate_options(raft::consistency_level::quorum_ack);
    opts.set_force_flush();
    return opts;
}
} // namespace

namespace datalake::coordinator {

coordinator_stm::coordinator_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>("datalake_coordinator_stm.snapshot", logger, raft) {}

ss::future<> coordinator_stm::do_apply(const model::record_batch&) {
    co_return;
}

model::offset coordinator_stm::max_collectible_offset() { return {}; }

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

ss::future<iobuf> coordinator_stm::take_snapshot(model::offset) {
    co_return iobuf{};
}

ss::future<add_translated_data_files_reply>
coordinator_stm::add_translated_data_file(
  add_translated_data_files_request req) {
    if (!co_await sync(wait_timeout)) {
        co_return add_translated_data_files_reply{coordinator_errc::not_leader};
    }
    auto synced_term = _insync_term;
    auto& partition_state = _all_files[req.tp.topic][req.tp.partition];
    if (partition_state.request_in_progress) {
        co_return add_translated_data_files_reply{
          coordinator_errc::concurrent_requests};
    }
    partition_state.request_in_progress = true;
    auto guard = ss::defer(
      [&partition_state] { partition_state.request_in_progress = true; });

    if (!partition_state.files.empty()) {
        auto& last = partition_state.files.back();
        if (last.translator_term > req.translator_term) {
            co_return add_translated_data_files_reply{coordinator_errc::fenced};
        }
        for (auto& file : req.files) {
            if (file.begin_offset <= last.end_offset) {
                co_return add_translated_data_files_reply{
                  coordinator_errc::stale};
            }
        }
    }
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    for (auto& entry : req.files) {
        builder.add_raw_kv(std::nullopt, serde::to_iobuf(entry));
    }
    auto rdr = model::make_memory_record_batch_reader(
      std::move(builder).build());
    auto result = co_await _raft->replicate(
      synced_term, std::move(rdr), make_replicate_options());
    auto timeout = model::timeout_clock::now() + wait_timeout;
    if (result && co_await wait_no_throw(result.value().last_offset, timeout)) {
        co_return add_translated_data_files_reply{coordinator_errc::ok};
    }
    if (_raft->term() == synced_term) {
        co_await _raft->step_down(
          "add_translated_data_file failed, stepping down.");
    }
    co_return add_translated_data_files_reply(coordinator_errc::not_leader);
}

ss::future<fetch_latest_data_file_reply>
coordinator_stm::fetch_latest_data_file(fetch_latest_data_file_request req) {
    if (!co_await sync(wait_timeout)) {
        co_return fetch_latest_data_file_reply{coordinator_errc::not_leader};
    }
    auto response = fetch_latest_data_file_reply{coordinator_errc::ok};
    auto it = _all_files.find(req.tp.topic);
    if (it != _all_files.end() && it->second.contains(req.tp.partition)) {
        auto& state = it->second[req.tp.partition];
        if (!state.files.empty()) {
            response.entry = state.files.back();
        }
    }
    co_return response;
}

bool stm_factory::is_applicable_for(const storage::ntp_config& config) const {
    const auto& ntp = config.ntp();
    return (ntp.ns == model::datalake_coordinator_nt.ns)
           && (ntp.tp.topic == model::datalake_coordinator_topic);
}

void stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto stm = builder.create_stm<coordinator_stm>(datalake_log, raft);
    raft->log()->stm_manager()->add_stm(stm);
}

} // namespace datalake::coordinator
