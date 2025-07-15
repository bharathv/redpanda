/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cluster_link/logger.h"
#include "cluster_link/replication/deps.h"
#include "ssx/semaphore.h"
#include "utils/named_type.h"
#include "utils/notification_list.h"
#include "utils/prefix_logger.h"

namespace cluster_link::replication {

/**
 * Responsible for replicating a remote partition to the local cluster. An
 * instance of this class exists for each local partition leader active on the
 * current shard.
 *
 * Architecture:
 * 
 *   [Caller] <----------------------------------------+
 *       |                                             |
 *       | enqueue_for_replication()                   | notification_cb(offset, error)
 *       v                                             | (on error)
 *   +---------+                       +----------+    |
 *   | Memory  |----queue_full-------> | Callback |---+
 *   |Semaphore|                       |Notification|  |
 *   +---------+                       +----------+    |
 *       |                                             |
 *       |                                             |
 *   +---+----+     do_replicate()    +----------+     |  replication     +----------+
 *   | Pending|  ------------------>  | data_sink| --+---------------->  | Local    |
 *   | Queue  |     (async loop)      |          |                      | Partition|
 *   +--------+                       +----------+                       +----------+
 *
 * Flow:
 * 1. Batches enqueued to _pending queue (memory-limited by _memory semaphore)
 * 2. do_replicate() loop processes queue asynchronously 
 * 3. Only replication errors/queue full events trigger notifications with offset
 * 4. Errors propagated back to original caller via notification_cb for backoff/retry logic
 */

class partition_replicator {
public:
    explicit partition_replicator(
      const model::ntp& ntp,
      model::term_id,
      std::unique_ptr<data_sink> sink,
      size_t max_buffered_bytes);

    using notification_offset = kafka::offset;
    enum class notification { queue_full, reset_source };
    using notification_id = named_type<int32_t, struct notification_id_t>;
    using notification_cb
      = ss::noncopyable_function<void(notification_offset, notification)>;

    ss::future<> start(notification_cb);

    ss::future<> stop();

    void enqueue_for_replication(chunked_vector<model::record_batch>) noexcept;

private:
    ss::future<> do_replicate();
    void do_notify(kafka::offset, notification);
    void handle_replication_result(
      kafka::offset begin, kafka::offset end, replication_errc);

    prefix_logger _log;
    ss::gate _gate;
    std::unique_ptr<data_sink> _sink;
    ssx::semaphore _memory;
    chunked_vector<model::record_batch> _pending;
    // memory units for the pending batches
    ssx::semaphore_units _pending_units;
    ss::condition_variable _pending_cv;
    std::optional<notification_id> _subscription;
    notification_list<notification_cb, notification_id> _subscribers;
    bool _empty_queue_monitor;
};

} // namespace cluster_link::replication
