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
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/logger.h"
#include "raft/group_manager.h"

#include <memory>

namespace datalake {

datalake_manager::datalake_manager(
  model::node_id self,
  ss::sharded<raft::group_manager>* group_mgr,
  ss::sharded<cluster::partition_manager>* partition_mgr,
  ss::sharded<cluster::topic_table>* topic_table,
  ss::sharded<cluster::topics_frontend>* topics_frontend,
  ss::sharded<cluster::partition_leaders_table>* leaders,
  ss::sharded<cluster::shard_table>* shards,
  ss::sharded<coordinator::frontend>* frontend,
  ss::sharded<ss::abort_source>* as,
  ss::scheduling_group sg,
  [[maybe_unused]] size_t memory_limit)
  : _self(self)
  , _group_mgr(group_mgr)
  , _partition_mgr(partition_mgr)
  , _topic_table(topic_table)
  , _topics_frontend(topics_frontend)
  , _leaders(leaders)
  , _shards(shards)
  , _coordinator_frontend(frontend)
  , _as(as)
  , _sg(sg)
  , _effective_max_translator_buffered_data(
      std::min(memory_limit, max_translator_buffered_data))
  , _parallel_translations(
      size_t(
        std::floor(memory_limit / _effective_max_translator_buffered_data)),
      "datalake_parallel_translations") {
    // Handle leadership changes
    auto leadership_registration
      = _group_mgr->local().register_leadership_notification(
        [this](
          raft::group_id group,
          ::model::term_id,
          std::optional<::model::node_id>) { on_group_notifiction(group); });

    // Handle topic properties changes (iceberg.enabled=true/false)
    auto topic_properties_registration
      = _topic_table->local().register_delta_notification(
        [this](cluster::topic_table::delta_range_t range) {
            for (auto& entry : range) {
                if (
                  entry.type
                  == cluster::topic_table_delta_type::properties_updated) {
                    on_group_notifiction(entry.group);
                }
            }
        });
    _deregistrations.reserve(2);
    _deregistrations.emplace_back([this, leadership_registration] {
        _group_mgr->local().unregister_leadership_notification(
          leadership_registration);
    });
    _deregistrations.emplace_back([this, topic_properties_registration] {
        _topic_table->local().unregister_delta_notification(
          topic_properties_registration);
    });
}

ss::future<> datalake_manager::stop() {
    auto f = _gate.close();
    _deregistrations.clear();
    co_await std::move(f);
}

void datalake_manager::on_group_notifiction(raft::group_id group) {
    auto partition = _partition_mgr->local().partition_for(group);
    const auto& topic_cfg = _topic_table->local().get_topic_cfg(
      model::topic_namespace_view{partition->ntp()});
    if (!partition || !topic_cfg) {
        return;
    }
    vlog(
      datalake_log.debug,
      "notification for partition: {}, leader: {}, iceberg: {}",
      partition->ntp(),
      partition->is_leader(),
      topic_cfg->properties.iceberg_enabled);
    if (!partition->is_leader() || !topic_cfg->properties.iceberg_enabled) {
        if (_translators.contains(group)) {
            ssx::spawn_with_gate(_gate, [this, partition] {
                return stop_translator(partition->group());
            });
        }
        return;
    }
    ssx::spawn_with_gate(
      _gate, [this, partition] { return start_translator(partition); });
}

ss::future<> datalake_manager::start_translator(
  ss::lw_shared_ptr<cluster::partition> partition) {
    // stop any older instances
    co_await stop_translator(partition->group());
    vlog(datalake_log.info, "Starting translator for: {}", partition->ntp());
    auto translator = std::make_unique<translation::partition_translator>(
      partition,
      _coordinator_frontend,
      _sg,
      _effective_max_translator_buffered_data,
      _parallel_translations);
    _translators.emplace(partition->group(), std::move(translator));
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

} // namespace datalake
