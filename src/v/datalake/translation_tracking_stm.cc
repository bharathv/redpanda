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

#include "datalake/translation_tracking_stm.h"

#include "datalake/logger.h"
#include "datalake/types.h"

namespace datalake::stm {

translation_tracking_stm::translation_tracking_stm(
  ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>("datalake_stm.snapshot", logger, raft) {}

ss::future<::model::offset>
translation_tracking_stm::highest_translated_offset() {
    return raft::persisted_stm<>::sync(::model::timeout_clock::duration::max())
      .then([this](bool success) {
          if (!success) {
              return ss::make_exception_future<::model::offset>(
                ss::timed_out_error{});
          }
          return ssx::now(_highest_translated_offset);
      });
}

ss::future<>
translation_tracking_stm::do_apply(const ::model::record_batch& batch) {
    if (
      batch.header().type
      != ::model::record_batch_type::datalake_translation_metadata) {
        co_return;
    }
    auto records = batch.copy_records();
    vassert(
      records.size() == 1,
      "Invalid metadata in batch {}, size: {}",
      batch.header(),
      records.size());
    auto& r = *records.begin();
    auto val = serde::from_iobuf<model::translation_metadata_value>(
      r.release_value());
    _highest_translated_offset = val.highest_translated_offset;
}

::model::offset translation_tracking_stm::max_collectible_offset() {
    return _highest_translated_offset;
}

ss::future<> translation_tracking_stm::apply_local_snapshot(
  raft::stm_snapshot_header, iobuf&& bytes) {
    _highest_translated_offset
      = serde::from_iobuf<snapshot>(std::move(bytes)).highest_translated_offset;
    co_return;
}

ss::future<raft::stm_snapshot> translation_tracking_stm::take_local_snapshot(
  ssx::semaphore_units apply_units) {
    auto snapshot_offset = last_applied_offset();
    snapshot snap{.highest_translated_offset = _highest_translated_offset};
    apply_units.return_all();
    iobuf result;
    co_await serde::write_async(result, snap);
    co_return raft::stm_snapshot::create(0, snapshot_offset, std::move(result));
}

ss::future<> translation_tracking_stm::apply_raft_snapshot(const iobuf&) {
    co_return;
}

ss::future<iobuf> translation_tracking_stm::take_snapshot(::model::offset) {
    co_return iobuf{};
}

bool stm_factory::is_applicable_for(const storage::ntp_config& config) const {
    return ::model::is_user_topic(config.ntp()) && config.datalake_enabled();
}

void stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto stm = builder.create_stm<translation_tracking_stm>(datalake_log, raft);
    raft->log()->stm_manager()->add_stm(stm);
}

} // namespace datalake::stm
