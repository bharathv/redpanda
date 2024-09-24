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

#include "base/seastarx.h"
#include "base/units.h"
#include "cluster/fwd.h"
#include "raft/fundamental.h"
#include "raft/fwd.h"
#include "ssx/semaphore.h"

#include <seastar/core/gate.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

namespace datalake {

namespace coordinator {
class frontend;
};

/*
 * Per shard instance responsible for launching and synchronizing all datalake
 * related tasks like file format translation, frontend etc.
 */
class datalake_manager : public ss::peering_sharded_service<datalake_manager> {
public:
    datalake_manager(
      model::node_id self,
      ss::sharded<raft::group_manager>*,
      ss::sharded<cluster::partition_manager>*,
      ss::sharded<cluster::topics_frontend>*,
      ss::sharded<cluster::partition_leaders_table>*,
      ss::sharded<cluster::shard_table>*,
      ss::sharded<coordinator::frontend>*,
      ss::sharded<ss::abort_source>*,
      ss::scheduling_group sg,
      size_t memory_limit);

    ss::future<> stop();

private:
    ss::future<> stop_translator(raft::group_id);
    model::node_id _self;
    ss::sharded<raft::group_manager>* _group_mgr;
    ss::sharded<cluster::partition_manager>* _partition_mgr;
    ss::sharded<cluster::topics_frontend>* _topics_frontend;
    ss::sharded<cluster::partition_leaders_table>* _leaders;
    ss::sharded<cluster::shard_table>* _shards;
    ss::sharded<coordinator::frontend>* _coordinator_frontend;
    ss::sharded<ss::abort_source>* _as;
    ss::scheduling_group _sg;
    ss::gate _gate;
};

} // namespace datalake
