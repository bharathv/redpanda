/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "base/units.h"
#include "cluster/fwd.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "datalake/fwd.h"
#include "datalake/translation/partition_translator.h"
#include "features/fwd.h"
#include "raft/fundamental.h"
#include "raft/fwd.h"
#include "ssx/semaphore.h"

#include <seastar/core/gate.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

namespace cloud_io {
class remote;
}

namespace datalake {

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
      ss::sharded<cluster::topic_table>*,
      ss::sharded<cluster::topics_frontend>*,
      ss::sharded<cluster::partition_leaders_table>*,
      ss::sharded<cluster::shard_table>*,
      ss::sharded<features::feature_table>*,
      ss::sharded<coordinator::frontend>*,
      ss::sharded<cloud_io::remote>*,
      ss::sharded<ss::abort_source>*,
      ss::scheduling_group sg,
      size_t memory_limit);

    ss::future<> start();
    ss::future<> stop();

private:
    using translator = std::unique_ptr<translation::partition_translator>;
    using translator_map = chunked_hash_map<model::ntp, translator>;

    void on_group_notification(const model::ntp&);
    ss::future<> start_translator(ss::lw_shared_ptr<cluster::partition>);
    ss::future<> stop_translator(const model::ntp&);

    model::node_id _self;
    ss::sharded<raft::group_manager>* _group_mgr;
    ss::sharded<cluster::partition_manager>* _partition_mgr;
    ss::sharded<cluster::topic_table>* _topic_table;
    ss::sharded<cluster::topics_frontend>* _topics_frontend;
    ss::sharded<cluster::partition_leaders_table>* _leaders;
    ss::sharded<cluster::shard_table>* _shards;
    ss::sharded<features::feature_table>* _features;
    ss::sharded<coordinator::frontend>* _coordinator_frontend;
    std::unique_ptr<datalake::cloud_data_io> _cloud_data_io;
    ss::sharded<ss::abort_source>* _as;
    ss::scheduling_group _sg;
    ss::gate _gate;

    size_t _effective_max_translator_buffered_data;
    std::unique_ptr<ssx::semaphore> _parallel_translations;
    translator_map _translators;
    using deferred_action = ss::deferred_action<std::function<void()>>;
    std::vector<deferred_action> _deregistrations;
    config::binding<std::chrono::milliseconds> _translation_ms_conf;

    // Translation requires buffering data batches in memory for efficient
    // output representation, this controls the maximum bytes buffered in memory
    // before the output is flushed.
    static constexpr size_t max_translator_buffered_data = 64_MiB;
};

} // namespace datalake
