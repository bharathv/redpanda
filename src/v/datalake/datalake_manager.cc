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

#include "datalake/datalake_manager.h"

#include "cluster/partition_manager.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/logger.h"
#include "datalake/translation_tracking_stm.h"
#include "raft/group_manager.h"

#include <memory>

namespace datalake {

datalake_manager::datalake_manager(
  ::model::node_id self,
  ss::sharded<raft::group_manager>* group_mgr,
  ss::sharded<cluster::partition_manager>* partition_mgr,
  ss::sharded<cluster::topics_frontend>* topics_frontend,
  ss::sharded<cluster::partition_leaders_table>* leaders,
  ss::sharded<cluster::shard_table>* shards,
  ss::sharded<ss::abort_source>* as,
  ss::scheduling_group sg,
  size_t memory_limit)
  : _self(self)
  , _group_mgr(group_mgr)
  , _partition_mgr(partition_mgr)
  , _topics_frontend(topics_frontend)
  , _leaders(leaders)
  , _shards(shards)
  , _as(as)
  , _sg(sg)
  , _effective_max_translator_buffered_data(
      std::min(memory_limit, max_translator_buffered_data))
  , _parallel_translations{
      size_t(
        std::floor(memory_limit / _effective_max_translator_buffered_data)),
      "datalake_parallel_translations"} {}

ss::future<> datalake_manager::start_translator(raft::group_id group) {
    // stop any older instances
    co_await stop_translator(group);
    auto partition = _partition_mgr->local().partition_for(group);
    if (!partition) {
        vlog(
          datalake_log.error,
          "No partition for group: {} on shard, ignoring translation request",
          group);
        co_return;
    }

    auto stm = partition->raft()
                 ->stm_manager()
                 ->get<datalake::translation::translation_tracking_stm>();
    auto translator = std::make_unique<translation::partition_translator>(
      partition->ntp(),
      std::move(stm),
      &_coordinator_frontend,
      _sg,
      _effective_max_translator_buffered_data,
      _parallel_translations,
      _as->local());
    _translators.emplace(group, std::move(translator));
}

ss::future<> datalake_manager::stop_translator(raft::group_id group) {
    auto it = _translators.find(group);
    if (it == _translators.end()) {
        co_return;
    }
    auto translator = std::move(it->second);
    _translators.erase(it);
    co_await translator->stop();
}

void datalake_manager::on_leadership_change(
  raft::group_id group,
  ::model::term_id,
  std::optional<::model::node_id> leader_id) {
    if (_as->local().abort_requested()) {
        return;
    }
    if (leader_id == _self) {
        ssx::spawn_with_gate(
          _gate, [this, group] { return start_translator(group); });
        return;
    }
    // lost leadership
    ssx::spawn_with_gate(
      _gate, [this, group] { return stop_translator(group); });
}

ss::future<> datalake_manager::start() {
    co_await _coordinator_frontend.start(
      _self, _group_mgr, _partition_mgr, _topics_frontend, _leaders, _shards);
    auto registration_id = _group_mgr->local().register_leadership_notification(
      [this](
        raft::group_id group,
        ::model::term_id term,
        std::optional<::model::node_id> leader) {
          on_leadership_change(group, term, leader);
      });
    _unregister_notifcation = std::make_unique<deferred_action>(
      [this, registration_id]() {
          _group_mgr->local().unregister_leadership_notification(
            registration_id);
      });
}

ss::future<> datalake_manager::stop() {
    _unregister_notifcation.reset();
    co_await _coordinator_frontend.stop();
    co_await _gate.close();
    co_await ss::max_concurrent_for_each(
      _translators.begin(), _translators.end(), 32, [this](auto& it) {
          return stop_translator(it.first);
      });
}

} // namespace datalake
