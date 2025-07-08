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
#include "container/chunked_hash_map.h"
#include "kafka/client/cluster.h"
#include "kafka/client/direct_consumer/api_types.h"

#include <seastar/core/queue.hh>
#include <seastar/core/rwlock.hh>

namespace kafka {
struct metadata_response_data;
namespace client {

class fetcher;
class data_queue;
/**
 * Simple direct Kafka consumer. It allows user to subscribe to a list of
 * topic partitions. The consumer maintains broker fetch session, it reacts for
 * the leadership changes and metadata updates.
 * Fetch responses from all the brokers are exposed through `fetch_next()`.
 * The fetch_next() returns whenever any ready to be consumer are available.
 *
 * Implementation details:
 * NOTE:
 * Fetcher and data queue classes should not be used directly, they are
 * implementation details of the direct consumer, they were placed into separate
 * files for readability.
 *
 * Direct consumer uses fetcher class to fetch data from the brokers. The
 * fetcher is responsible for fetching data from a single broker. It maintains a
 * list of partitions and corresponding fetch offsets. The fetcher loop
 * constantly queries the broker for data and updates the fetch offsets based on
 * the received data. Fetcher is also responsible for fetching offsets to apply
 * the reset policy. Fetcher puts the fetched data into the parent consumer's
 * data queue. The queue is then accessed through the `fetch_next()`
 * method.
 *
 * The direct consumer maintains a list of partition subscriptions.
 * When the subscription list is changed the consumer updates the
 * fetchers state. In order to maintain partition batches ordering the invariant
 * is held that one partition is never assigned to multiple fetchers at the same
 * time.
 *
 * Fetchers are 'blocked' from fetching while the consumer is updating their
 * assigned partitions. This is done by acquiring a write lock on the shared
 * state lock that is being held by the fetchers in the fetch loop.
 *
 */
class direct_consumer {
public:
    struct configuration {
        int32_t min_bytes{1};
        int32_t max_fetch_size{512_KiB};
        int32_t partition_max_bytes{128_KiB};
        offset_reset_policy reset_policy = offset_reset_policy::earliest;
        std::chrono::milliseconds max_wait_time{100};
        model::isolation_level isolation_level
          = model::isolation_level::read_uncommitted;
        // queue settings
        size_t max_buffered_bytes{10_MiB};
        size_t max_buffered_elements{10};
        friend std::ostream& operator<<(std::ostream&, const configuration&);
    };

    direct_consumer(cluster& cluster, configuration cfg);

    ~direct_consumer();
    /**
     * Stops consumer
     */
    ss::future<> stop();

    /**
     * Returns all data available to fetch. If the timeout passed to this method
     * is reached and there were no data to consume it will return an empty
     * vector.
     * The method returns an error whenever the consumer receives a
     * non-retriable error from any of the brokers.
     */
    ss::future<fetches> fetch_next(std::chrono::milliseconds timeout);

    /**
     * Assign partitions to be fetched from, if partition is already being
     * fetched from and is included in the vector its fetch offset will be
     * updated
     */
    ss::future<> assign_partitions(chunked_vector<topic_assignment>);

    /**
     * Removes listed topics from assignment.
     *
     * NOTE: if the topic is not present in the current assignment it will be
     * ignored.
     */
    ss::future<> unassign_topics(chunked_vector<model::topic> topics);

    /**
     * Removes listed topic partitions from assignment.
     *
     * NOTE: if the topic is not present in the current assignment it will be
     * ignored.
     */
    ss::future<> unassign_partitions(
      chunked_vector<model::topic_partition> topic_partitions);

    /**
     *  Updates consumer configuration, the configuration will be update when
     * for the next fetch request.
     */
    void update_configuration(configuration cfg);

private:
    friend class fetcher;
    void on_metadata_update(const metadata_response_data&);

    ss::future<> handle_metadata_update();
    ss::future<> update_fetchers(
      chunked_hash_map<model::topic, chunked_vector<model::partition_id>>
        removals
      = {});

    fetcher& get_fetcher(model::node_id id);

    cluster* _cluster;

    offset_reset_policy _reset_policy
      = offset_reset_policy::earliest; // default to earliest

    configuration _config;
    struct subscription {
        std::optional<model::node_id> current_fetcher;
        std::optional<model::offset> fetch_offset;
    };

    topic_partition_map<subscription> _subscriptions;
    chunked_hash_map<model::node_id, std::unique_ptr<fetcher>> _broker_fetchers;
    std::unique_ptr<data_queue> _fetched_data_queue;
    ss::condition_variable _data_available;
    /**
     * We use a read-write lock here as a convenient way to synchronize
     * access to the fetchers. The write lock is held when the fetchers needs to
     * be updated. f.e. subscription change or metadata update.
     * Fetchers themselves hold a read lock when they are fetching data.
     * Holding write lock effectively blocks all fetchers from fetching
     * data, so they will not interfere with the update and will 'see' all the
     * updates at once.
     */
    ss::rwlock _fetchers_lock;
    cluster::callback_id _metadata_callback_id;
    ss::gate _gate;
};
} // namespace client
} // namespace kafka
