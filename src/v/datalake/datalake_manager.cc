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
#include "raft/group_manager.h"

#include <memory>

namespace datalake {

datalake_manager::datalake_manager(
  model::node_id self,
  ss::sharded<raft::group_manager>* group_mgr,
  ss::sharded<cluster::partition_manager>* partition_mgr,
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
  , _topics_frontend(topics_frontend)
  , _leaders(leaders)
  , _shards(shards)
  , _coordinator_frontend(frontend)
  , _as(as)
  , _sg(sg) {}

ss::future<> datalake_manager::stop() { co_await _gate.close(); }

} // namespace datalake
