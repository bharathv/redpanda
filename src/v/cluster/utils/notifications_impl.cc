/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/partition_manager.h"
#include "cluster/topic_table.h"
#include "cluster/utils/notifications.h"
#include "raft/group_manager.h"

namespace cluster {

class notifications_impl final : public notifications {
public:
    notifications_impl(
      ss::sharded<raft::group_manager>& group_mgr,
      ss::sharded<cluster::partition_manager>& partition_mgr,
      ss::sharded<cluster::topic_table>& topic_table)
      : _group_mgr(group_mgr)
      , _partition_mgr(partition_mgr)
      , _topic_table(topic_table) {}

    notification_id_type
    register_leadership_notification(leader_cb_t cb) final {
        auto id = _group_mgr.local().register_leadership_notification(
          [this, cb = std::move(cb)](
            raft::group_id group,
            model::term_id,
            std::optional<model::node_id>) mutable {
              auto partition = _partition_mgr.local().partition_for(group);
              if (partition) {
                  cb(partition->ntp(), partition->is_leader());
              }
          });
        return notification_id_type{id()};
    }

    void unregister_leadership_notification(notification_id_type id) final {
        _group_mgr.local().unregister_leadership_notification(
          raft::group_manager_notification_id{id()});
    }

    notification_id_type
    register_partition_manage_notification(registration_change_cb_t cb) final {
        return _partition_mgr.local().register_manage_notification(
          model::kafka_namespace,
          [cb = std::move(cb)](
            const ss::lw_shared_ptr<cluster::partition>& partition) {
              if (partition) {
                  cb(partition->ntp());
              }
          });
    }
    void
    unregister_partition_manage_notification(notification_id_type id) final {
        _partition_mgr.local().unregister_manage_notification(id);
    }

    notification_id_type register_partition_unmanage_notification(
      registration_change_cb_t cb) final {
        return _partition_mgr.local().register_unmanage_notification(
          model::kafka_namespace,
          [cb = std::move(cb)](model::topic_partition_view tp) {
              model::ntp ntp{model::kafka_namespace, tp.topic, tp.partition};
              cb(ntp);
          });
    }

    void
    unregister_partition_unmanage_notification(notification_id_type id) final {
        _partition_mgr.local().unregister_unmanage_notification(id);
    }

    notification_id_type
    register_partition_properties_changed(properties_change_cb_t cb) final {
        return _topic_table.local().register_ntp_delta_notification(
          [this,
           cb = std::move(cb)](cluster::topic_table::ntp_delta_range_t range) {
              const auto& topic_table = _topic_table.local();
              for (const auto& entry : range) {
                  if (
                    entry.type
                    == cluster::topic_table_ntp_delta_type::
                      properties_updated) {
                      const auto& cfg = topic_table.get_topic_cfg(
                        model::topic_namespace_view{entry.ntp});
                      if (cfg) {
                          cb(entry.ntp, cfg.value());
                      }
                  }
              }
          });
    }

    void
    unregister_partition_properties_changed(notification_id_type id) final {
        _topic_table.local().unregister_ntp_delta_notification({id});
    }

private:
    ss::sharded<raft::group_manager>& _group_mgr;
    ss::sharded<cluster::partition_manager>& _partition_mgr;
    ss::sharded<cluster::topic_table>& _topic_table;
};

std::unique_ptr<cluster::notifications> make_notifications(
  ss::sharded<raft::group_manager>& group_mgr,
  ss::sharded<cluster::partition_manager>& partition_mgr,
  ss::sharded<cluster::topic_table>& topic_table) {
    return std::make_unique<notifications_impl>(
      group_mgr, partition_mgr, topic_table);
}
} // namespace cluster
