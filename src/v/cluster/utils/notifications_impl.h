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

#include "cluster/fwd.h"
#include "cluster/utils/notifications.h"
#include "raft/fwd.h"
#include "seastar/core/sharded.hh"

namespace cluster {

static std::unique_ptr<notifications> make_default(
  ss::sharded<raft::group_manager>&,
  ss::sharded<cluster::partition_manager>&,
  ss::sharded<cluster::topic_table>&);
}
