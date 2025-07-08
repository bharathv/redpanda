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
#include "container/intrusive_list_helpers.h"
#include "kafka/client/direct_consumer/api_types.h"
#include "kafka/client/direct_consumer/data_queue.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/list_offset.h"
#include "model/fundamental.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/core/rwlock.hh>
namespace kafka::client {
class direct_consumer;

struct fetch_session_state {
    enum class state {
        none,
        need_full_fetch,
        incremental_fetch,
        needs_close,
    };
    kafka::fetch_session_id _fetch_session_id{kafka::invalid_fetch_session_id};
    kafka::fetch_session_epoch _fetch_session_epoch{
      kafka::initial_fetch_session_epoch};

    void update_fetch_session(kafka::fetch_session_id id) {
        _fetch_session_id = id;
        _fetch_session_epoch++;
    }
};

struct partition_fetch_state {
    model::partition_id partition_id;
    std::optional<model::offset> fetch_offset;
    model::offset high_watermark;
    leader_epoch current_leader_epoch{kafka::invalid_leader_epoch};
    intrusive_list_hook _hook;

    bool include_in_fetch_request() const { return fetch_offset.has_value(); }
};
/**
 * Class responsible for fetching data from a single broker. It is maintaining a
 * list of partitions and corresponding fetch offsets. The fetcher loop
 * constantly querying the broker for data and updates the fetch offsets based
 * on the received data. Fetcher is also responsible for fetching offsets to
 * apply the reset policy.
 *
 * Fetcher put the fetched data into the parent consumer's data queue.
 *
 * The fetcher relies on the parent consumer concurrency control, it holds a
 * read lock every time it is dispatching the fetch request. This shared locking
 * scheme prevents the same partition to be fetched by multiple fetchers at the
 * same time. And guarantees that the direct consumer `fetch_next()` will not
 * reorder partition messages.
 *
 * TODO:
 * - support incremental fetches
 * - support leader epochs
 */
class fetcher {
public:
    fetcher(direct_consumer* parent, model::node_id id);
    void start();

    ss::future<> stop();

    ss::future<> assign_partition(
      model::topic_partition_view, std::optional<model::offset>);
    ss::future<std::optional<model::offset>>
      unassign_partition(model::topic_partition_view);

    bool is_idle() const { return _partitions.empty(); }

private:
    using state_list
      = intrusive_list<partition_fetch_state, &partition_fetch_state::_hook>;
    struct partitions_to_process {
        model::topic topic;
        state_list to_include_in_fetch;
        state_list to_list_offsets;

        bool empty() const {
            return to_include_in_fetch.empty() && to_list_offsets.empty();
        }
    };

    struct fetch_response_content {
        chunked_vector<fetched_topic_data> topics;
        size_t total_bytes{0};
        bool needs_metadata_update{false};
    };

    struct state_lock_holder {
        state_lock_holder(ss::rwlock::holder h, ssx::semaphore_units u)
          : holder(std::move(h))
          , units(std::move(u)) {}
        ss::rwlock::holder holder;
        ssx::semaphore_units units;
        void return_all() {
            holder.return_all();
            units.return_all();
        }
    };

    ss::future<api_version> get_fetch_request_version() const;
    ss::future<api_version> get_list_offsets_request_version() const;
    ss::future<> do_fetch();
    ss::future<chunked_vector<partitions_to_process>> collect_partitions();
    ss::future<kafka::error_code>
    maybe_initialise_fetch_offsets(chunked_vector<partitions_to_process>&);
    ss::future<fetch_request>
      make_fetch_request(chunked_vector<partitions_to_process>);

    ss::future<kafka_result<fetch_response_content>>
    process_fetch_response(fetch_response resp);
    ss::future<state_lock_holder> lock_state();
    void maybe_update_fetch_offset(
      const model::topic&, model::partition_id, model::offset);

    ss::future<kafka_result<chunked_vector<topic_partition_offsets>>>
      do_list_offsets(list_offsets_request);

    ss::rwlock& shared_state_lock();
    data_queue& queue();
    prefix_logger& logger();

    direct_consumer* _parent;
    model::node_id _id;
    fetch_session_state _session_state;
    topic_partition_map<partition_fetch_state> _partitions;
    ss::condition_variable _partitions_updated;
    ss::gate _gate;
    mutex _state_lock;
    ss::abort_source _as;
};
} // namespace kafka::client
