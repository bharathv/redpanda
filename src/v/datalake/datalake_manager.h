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

#pragma once

#include "base/seastarx.h"
#include "base/units.h"
#include "cluster/fwd.h"
#include "datalake/partition_translator.h"
#include "raft/fundamental.h"
#include "raft/fwd.h"
#include "ssx/semaphore.h"

#include <seastar/core/gate.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

#include <absl/container/btree_map.h>

namespace datalake {

namespace coordinator {
class frontend;
};

/*
 * Per shard instance responsible for launching and synchronizing all datalake
 * related tasks like file format translation, garbage colleciton etc.
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
      ss::sharded<ss::abort_source>*,
      ss::scheduling_group sg,
      size_t memory_limit);

    ss::future<> start();
    ss::future<> stop();

private:
    using translator = std::unique_ptr<translation::partition_translator>;
    using translator_map = absl::btree_map<raft::group_id, translator>;
    using deferred_action = ss::deferred_action<std::function<void()>>;

    void on_leadership_change(
      raft::group_id, model::term_id, std::optional<model::node_id>);

    ss::future<> start_translator(raft::group_id);
    ss::future<> stop_translator(raft::group_id);
    model::node_id _self;
    ss::sharded<raft::group_manager>* _group_mgr;
    ss::sharded<cluster::partition_manager>* _partition_mgr;
    ss::sharded<cluster::topics_frontend>* _topics_frontend;
    ss::sharded<cluster::partition_leaders_table>* _leaders;
    ss::sharded<cluster::shard_table>* _shards;
    ss::sharded<ss::abort_source>* _as;
    ss::scheduling_group _sg;
    ss::gate _gate;
    translator_map _translators;
    std::unique_ptr<deferred_action> _unregister_notifcation;
    size_t _effective_max_translator_buffered_data;
    ssx::semaphore _parallel_translations;
    ss::sharded<coordinator::frontend> _coordinator_frontend;
    // Translation requires buffering data batches in memory for efficient
    // output representation, this controls the maximum bytes buffered in memory
    // before the output is flushed.
    static constexpr size_t max_translator_buffered_data = 128_MiB;
};

} // namespace datalake
