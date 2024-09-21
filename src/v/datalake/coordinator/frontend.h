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

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "datalake/coordinator/rpc_service.h"
#include "datalake/coordinator/types.h"
#include "model/namespace.h"
#include "raft/fwd.h"
#include "rpc/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace datalake::coordinator {

namespace rpc {
class service;
}

class frontend : public ss::peering_sharded_service<frontend> {
public:
    using local_only = ss::bool_class<struct local_only>;
    using proto_t = datalake::coordinator::rpc::impl::
      datalake_coordinator_rpc_client_protocol;

    frontend(
      ::model::node_id self,
      ss::sharded<raft::group_manager>*,
      ss::sharded<cluster::partition_manager>*,
      ss::sharded<cluster::topics_frontend>*,
      ss::sharded<cluster::partition_leaders_table>*,
      ss::sharded<cluster::shard_table>*);

    ss::future<> stop();

    ss::future<add_translated_data_file_reply>
      add_translated_data_file(add_translated_data_files_request, local_only);

    ss::future<fetch_latest_data_file_reply>
      fetch_latest_data_file(fetch_latest_data_file_request, local_only);

    ss::sharded<cluster::partition_manager>* partition_mgr() const {
        return _partition_mgr;
    }

    ss::sharded<cluster::topics_frontend>* topics() const {
        return _topics_frontend;
    }

    ss::sharded<cluster::partition_leaders_table>* leaders() const {
        return _leaders;
    }

    ss::sharded<::rpc::connection_cache>* connections() const {
        return _connection_cache;
    }

    ss::sharded<cluster::shard_table>* shard_table() const {
        return _shard_table;
    }

    ::model::node_id self() const { return _self; }

    ss::future<bool> ensure_topic_exists();

    std::optional<::model::partition_id>
    coordinator_partition(const ::model::topic_partition&) const;

private:
    ss::future<add_translated_data_file_reply> add_translated_data_file_locally(
      add_translated_data_files_request,
      const ::model::ntp& coordinator_partition,
      ss::shard_id);

    ss::future<fetch_latest_data_file_reply> fetch_latest_data_file_locally(
      fetch_latest_data_file_request,
      const ::model::ntp& coordinator_partition,
      ss::shard_id);

    ::model::node_id _self;
    ss::sharded<raft::group_manager>* _group_mgr;
    ss::sharded<cluster::partition_manager>* _partition_mgr;
    ss::sharded<cluster::topics_frontend>* _topics_frontend;
    ss::sharded<cluster::partition_leaders_table>* _leaders;
    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<::rpc::connection_cache>* _connection_cache;
    ss::gate _gate;
};
} // namespace datalake::coordinator
