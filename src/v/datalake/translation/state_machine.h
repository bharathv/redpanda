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
#include "raft/persisted_stm.h"

namespace datalake::translation {

/// Tracks the progress of datalake translation and clamps the collectible
/// offset to ensure the data is not GC-ed before translation is finished.
class translation_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "datalake_translation_stm";

    explicit translation_stm(ss::logger&, raft::consensus*);

    ss::future<> do_apply(const model::record_batch&) override;
    model::offset max_collectible_offset() override;
    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&& bytes) override;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) override;

    ss::future<> apply_raft_snapshot(const iobuf&) final;
    ss::future<iobuf> take_snapshot(model::offset) final;

    raft::consensus* raft() const { return _raft; }

private:
    struct snapshot
      : serde::envelope<snapshot, serde::version<0>, serde::compat_version<0>> {
        model::offset highest_translated_offset;
        auto serde_fields() { return std::tie(highest_translated_offset); }
    };
    model::offset _highest_translated_offset{};
};

class stm_factory : public cluster::state_machine_factory {
public:
    stm_factory() = default;
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(raft::state_machine_manager_builder&, raft::consensus*) final;
};

} // namespace datalake::translation
