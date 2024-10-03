/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
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
  ss::sharded<features::feature_table>* features,
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
  , _features(features)
  , _coordinator_frontend(frontend)
  , _as(as)
  , _sg(sg)
  , _effective_max_translator_buffered_data(
      std::min(memory_limit, max_translator_buffered_data))
  , _parallel_translations(std::make_unique<ssx::semaphore>(
      size_t(
        std::floor(memory_limit / _effective_max_translator_buffered_data)),
      "datalake_parallel_translations"))
  , _translation_ms_conf(config::shard_local_cfg()
                           .iceberg_translation_interval_ms_default.bind()) {}

ss::future<> datalake_manager::start() {
    // partition managed notification, this is particularly
    // relevant for cross core movements without a term change.
    auto partition_managed_notification
      = _partition_mgr->local().register_manage_notification(
        model::kafka_namespace,
        [this](ss::lw_shared_ptr<cluster::partition> new_partition) {
            on_group_notification(new_partition->ntp());
        });
    auto partition_unmanaged_notification
      = _partition_mgr->local().register_unmanage_notification(
        model::kafka_namespace, [this](model::topic_partition_view tp) {
            model::ntp ntp{model::kafka_namespace, tp.topic, tp.partition};
            ssx::spawn_with_gate(_gate, [this, ntp = std::move(ntp)] {
                return stop_translator(ntp);
            });
        });
    // Handle leadership changes
    auto leadership_registration
      = _group_mgr->local().register_leadership_notification(
        [this](
          raft::group_id group,
          ::model::term_id,
          std::optional<::model::node_id>) {
            auto partition = _partition_mgr->local().partition_for(group);
            on_group_notification(partition->ntp());
        });

    // Handle topic properties changes (iceberg.enabled=true/false)
    auto topic_properties_registration
      = _topic_table->local().register_ntp_delta_notification(
        [this](cluster::topic_table::ntp_delta_range_t range) {
            for (auto& entry : range) {
                if (
                  entry.type
                  == cluster::topic_table_ntp_delta_type::properties_updated) {
                    on_group_notification(entry.ntp);
                }
            }
        });

    _deregistrations.reserve(4);
    _deregistrations.emplace_back([this, partition_managed_notification] {
        _partition_mgr->local().unregister_manage_notification(
          partition_managed_notification);
    });
    _deregistrations.emplace_back([this, partition_unmanaged_notification] {
        _partition_mgr->local().unregister_unmanage_notification(
          partition_unmanaged_notification);
    });
    _deregistrations.emplace_back([this, leadership_registration] {
        _group_mgr->local().unregister_leadership_notification(
          leadership_registration);
    });
    _deregistrations.emplace_back([this, topic_properties_registration] {
        _topic_table->local().unregister_ntp_delta_notification(
          topic_properties_registration);
    });
    _translation_ms_conf.watch([this] {
        ssx::spawn_with_gate(_gate, [this]() {
            for (const auto& [group, _] : _translators) {
                on_group_notification(group);
            }
        });
    });
    return ss::make_ready_future<>();
}

ss::future<> datalake_manager::stop() {
    auto f = _gate.close();
    _deregistrations.clear();
    co_await ss::max_concurrent_for_each(
      _translators, 32, [](auto& entry) mutable {
          return entry.second->stop();
      });
    co_await std::move(f);
}

void datalake_manager::on_group_notification(const model::ntp& ntp) {
    auto partition = _partition_mgr->local().get(ntp);
    if (!partition || !model::is_user_topic(ntp)) {
        return;
    }
    const auto& topic_cfg = _topic_table->local().get_topic_cfg(
      model::topic_namespace_view{ntp});
    if (!topic_cfg) {
        return;
    }
    auto it = _translators.find(ntp);
    // todo(iceberg) handle topic / partition disabling
    if (!partition->is_leader() || !topic_cfg->properties.iceberg_enabled) {
        if (it != _translators.end()) {
            ssx::spawn_with_gate(_gate, [this, partition] {
                return stop_translator(partition->ntp());
            });
        }
        return;
    }
    // By now we know the partition is a leader and iceberg is enabled, so
    // there has to be a translator, spin one up if it doesn't already exist.
    if (it == _translators.end()) {
        ssx::spawn_with_gate(
          _gate, [this, partition] { return start_translator(partition); });
    } else {
        auto default_value
          = config::shard_local_cfg().iceberg_translation_interval_ms_default();
        // check if translation interval changed.
        auto target_interval
          = topic_cfg->properties.iceberg_translation_interval_ms.value_or(
            default_value);
        if (it->second->translation_interval() != target_interval) {
            it->second->reset_translation_interval(target_interval);
        }
    }
}

ss::future<> datalake_manager::start_translator(
  ss::lw_shared_ptr<cluster::partition> partition) {
    // stop any older instances
    co_await stop_translator(partition->ntp());
    auto translator = std::make_unique<translation::partition_translator>(
      partition,
      _coordinator_frontend,
      _features,
      partition->get_ntp_config().iceberg_translation_interval_ms(),
      _sg,
      _effective_max_translator_buffered_data,
      &_parallel_translations);
    _translators.emplace(partition->ntp(), std::move(translator));
}

ss::future<> datalake_manager::stop_translator(const model::ntp& ntp) {
    auto it = _translators.find(ntp);
    if (it == _translators.end()) {
        co_return;
    }
    auto translator = std::move(it->second);
    _translators.erase(it);
    co_await translator->stop();
}

} // namespace datalake
