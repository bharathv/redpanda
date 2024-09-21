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

#include "datalake/coordinator/frontend.h"

#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "datalake/coordinator/state_machine.h"
#include "datalake/logger.h"
#include "raft/group_manager.h"
#include "rpc/connection_cache.h"

namespace {
using proto_t = datalake::coordinator::frontend::proto_t;
using client
  = datalake::coordinator::rpc::impl::datalake_coordinator_rpc_client_protocol;

static constexpr std::chrono::seconds rpc_timeout{5};

template<auto Func, typename req_t>
// todo: add a concept
auto remote_dispatch(
  datalake::coordinator::frontend* instance,
  req_t request,
  ::model::node_id leader_id) {
    using resp_t = req_t::resp_t;
    return instance->connections()
      ->local()
      .with_node_client<proto_t>(
        instance->self(),
        ss::this_shard_id(),
        leader_id,
        rpc_timeout,
        [request = std::move(request)](proto_t proto) mutable {
            return (proto.*Func)(
              std::move(request),
              rpc::client_opts{model::timeout_clock::now() + rpc_timeout});
        })
      .then(&rpc::get_ctx_data<resp_t>)
      .then([leader_id](result<resp_t> r) {
          if (r.has_error()) {
              vlog(
                datalake::datalake_log.warn,
                "got error {} on coordinator {}",
                r.error().message(),
                leader_id);
              return resp_t{datalake::coordinator_errc::timeout};
          }
          return r.value();
      });
}

template<auto LocalFunc, auto RemoteFunc, typename req_t>
auto process(
  datalake::coordinator::frontend* instance, req_t req, bool local_only) {
    using resp_t = req_t::resp_t;
    return instance->ensure_topic_exists().then(
      [req = std::move(req), instance, local_only](bool exists) mutable {
          if (!exists) {
              return ss::make_ready_future<resp_t>(resp_t{
                datalake::coordinator_errc::coordinator_topic_not_exists});
          }
          auto cp = instance->coordinator_partition(req.topic_partition());
          ::model::ntp c_ntp{
            ::model::datalake_coordinator_nt.ns,
            ::model::datalake_coordinator_nt.tp,
            cp.value()};
          auto leader = instance->leaders()->local().get_leader(c_ntp);
          if (leader == instance->self()) {
              auto shard = instance->shard_table()->local().shard_for(c_ntp);
              if (shard) {
                  return (instance->*LocalFunc)(
                    std::move(req), std::move(c_ntp), shard.value());
              }
          } else if (leader && !local_only) {
              return remote_dispatch<RemoteFunc>(
                instance, std::move(req), leader.value());
          }
          return ss::make_ready_future<resp_t>(
            resp_t{datalake::coordinator_errc::not_leader});
      });
}

} // namespace

namespace datalake::coordinator {

frontend::frontend(
  ::model::node_id self,
  ss::sharded<raft::group_manager>* group_mgr,
  ss::sharded<cluster::partition_manager>* partition_mgr,
  ss::sharded<cluster::topics_frontend>* topics_frontend,
  ss::sharded<cluster::partition_leaders_table>* leaders,
  ss::sharded<cluster::shard_table>* shards)
  : _self(self)
  , _group_mgr(group_mgr)
  , _partition_mgr(partition_mgr)
  , _topics_frontend(topics_frontend)
  , _leaders(leaders)
  , _shard_table(shards) {}

ss::future<> frontend::stop() { return _gate.close(); }

std::optional<::model::partition_id>
frontend::coordinator_partition(const ::model::topic_partition& tp) const {
    const auto md
      = _topics_frontend->local().get_metadata_cache().get_topic_metadata_ref(
        ::model::datalake_coordinator_nt);
    if (!md) {
        return std::nullopt;
    }
    iobuf temp;
    write(temp, tp);
    auto bytes = iobuf_to_bytes(temp);
    auto partition = murmur2(bytes.data(), bytes.size())
                     % md->get().get_configuration().partition_count;
    return ::model::partition_id{static_cast<int32_t>(partition)};
}

ss::future<bool> frontend::ensure_topic_exists() {
    // todo: make these configurable.
    static constexpr int16_t default_replication_factor = 3;
    static constexpr int32_t default_coordinator_partitions = 3;

    auto& topics = _topics_frontend->local();
    const auto& metadata = topics.get_metadata_cache();

    if (metadata.get_topic_metadata_ref(::model::datalake_coordinator_nt)) {
        co_return true;
    }
    auto replication_factor = default_replication_factor;
    if (replication_factor > (int16_t)metadata.node_count()) {
        replication_factor = 1;
    }

    cluster::topic_configuration topic{
      ::model::datalake_coordinator_nt.ns,
      ::model::datalake_coordinator_nt.tp,
      default_coordinator_partitions,
      replication_factor};

    // todo: fix this by implementing on demand raft
    // snapshots.
    topic.properties.cleanup_policy_bitflags
      = ::model::cleanup_policy_bitflags::none;

    try {
        auto res = co_await topics.autocreate_topics(
          {std::move(topic)},
          config::shard_local_cfg().create_topic_timeout_ms());
        vassert(res.size() == 1, "expected exactly one result");
        if (
          res[0].ec != cluster::errc::success
          && res[0].ec != cluster::errc::topic_already_exists) {
            vlog(
              datalake::datalake_log.warn,
              "can not create topic: {} - error: {}",
              ::model::datalake_coordinator_nt,
              cluster::make_error_code(res[0].ec).message());
            co_return false;
        }
        co_return true;
    } catch (const std::exception_ptr& e) {
        vlog(
          datalake::datalake_log.warn,
          "can not create topic {} - exception: {}",
          ::model::datalake_coordinator_nt,
          e);
        co_return false;
    }
}

ss::future<add_translated_data_file_reply>
frontend::add_translated_data_file_locally(
  add_translated_data_files_request request,
  const ::model::ntp& coordinator_partition,
  ss::shard_id shard) {
    co_return co_await _partition_mgr->invoke_on(
      shard,
      [&coordinator_partition,
       req = std::move(request)](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(coordinator_partition);
          if (!partition) {
              return ssx::now(
                add_translated_data_file_reply{coordinator_errc::not_leader});
          }
          auto stm = partition->raft()->stm_manager()->get<coordinator_stm>();
          return stm->add_translated_data_file(std::move(req));
      });
}

ss::future<add_translated_data_file_reply> frontend::add_translated_data_file(
  add_translated_data_files_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::add_translated_data_file_locally,
      &client::add_translated_data_file>(
      this, std::move(request), bool(local_only_exec));
}

ss::future<fetch_latest_data_file_reply>
frontend::fetch_latest_data_file_locally(
  fetch_latest_data_file_request request,
  const ::model::ntp& coordinator_partition,
  ss::shard_id shard) {
    co_return co_await _partition_mgr->invoke_on(
      shard,
      [&coordinator_partition,
       req = std::move(request)](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(coordinator_partition);
          if (!partition) {
              return ssx::now(
                fetch_latest_data_file_reply{coordinator_errc::not_leader});
          }
          auto stm = partition->raft()->stm_manager()->get<coordinator_stm>();
          return stm->fetch_latest_data_file(std::move(req));
      });
}

ss::future<fetch_latest_data_file_reply> frontend::fetch_latest_data_file(
  fetch_latest_data_file_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::fetch_latest_data_file_locally,
      &client::fetch_latest_data_file>(
      this, std::move(request), bool(local_only_exec));
}

} // namespace datalake::coordinator
