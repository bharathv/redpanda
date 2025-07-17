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

#pragma once

#include "base/seastarx.h"
#include "cluster/notification.h"
#include "cluster/topic_configuration.h"
#include "model/fundamental.h"

#include <seastar/util/noncopyable_function.hh>

namespace cluster {

// Interface for partition level notifications.
class notifications {
public:
    // bool is true if the ntp local to this shard is the leader
    using leader_cb_t = ss::noncopyable_function<void(const model::ntp&, bool)>;
    using registration_change_cb_t
      = ss::noncopyable_function<void(const model::ntp&)>;
    using properties_change_cb_t = ss::noncopyable_function<void(
      const model::ntp&, const cluster::topic_configuration&)>;

    notifications() = default;
    notifications(const notifications&) = delete;
    notifications(notifications&&) = delete;
    notifications& operator=(const notifications&) = delete;
    notifications& operator=(notifications&&) = delete;

    virtual ~notifications() = default;

    virtual notification_id_type register_leadership_notification(leader_cb_t)
      = 0;
    virtual void unregister_leadership_notification(notification_id_type) = 0;

    virtual notification_id_type
      register_partition_manage_notification(registration_change_cb_t)
      = 0;
    virtual void unregister_partition_manage_notification(notification_id_type)
      = 0;

    virtual notification_id_type
      register_partition_unmanage_notification(registration_change_cb_t)
      = 0;
    virtual void
      unregister_partition_unmanage_notification(notification_id_type)
      = 0;

    virtual notification_id_type
      register_partition_properties_changed(properties_change_cb_t)
      = 0;
    virtual void unregister_partition_properties_changed(notification_id_type)
      = 0;
};
} // namespace cluster
