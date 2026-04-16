# Redpanda Architecture — Operational Model

## Overview

Redpanda is a Kafka-compatible streaming platform built on Seastar (a C++ framework for high-performance async I/O). Unlike Kafka, Redpanda runs without a JVM and uses a thread-per-core architecture.

## Core Concepts

### Shards

Redpanda uses Seastar's shard-per-core model. Each CPU core runs a single shard. Shards are isolated — they have their own memory, their own I/O queues, and their own partition assignments. Cross-shard communication uses Seastar's messaging (not shared memory).

This means:
- Diagnostic events are per-shard. A partition lives on one shard per broker.
- Resource pressure on one shard doesn't directly affect other shards, but can indirectly (e.g., disk is shared).
- When diagnosing, per-shard event logs show what happened on that specific core.

### Partitions and Raft Groups

Each partition is a Raft group. The Raft group has:
- A **leader** that handles all reads and writes for that partition
- **Followers** that replicate the leader's log
- A **replica set** — the set of brokers hosting replicas of this partition

Leadership and replica set membership are managed by the cluster controller.

### Controller

The controller is a single Raft group (partition 0 of the internal `controller` topic) that manages cluster metadata:
- Partition assignments (which brokers host which partitions)
- Broker membership (join, decommission, maintenance)
- Configuration changes
- Partition movements (the controller decides what to move and where)

The controller leader runs on one broker. If that broker goes down, a new controller leader is elected.

### Partition Balancer

The partition balancer runs on the controller leader. It:
1. Evaluates current partition distribution against constraints (rack awareness, disk usage, even distribution)
2. Decides which partitions to move and where
3. Issues partition movement commands to the controller

Balancer modes:
- `off` — no automatic rebalancing
- `node_add` — only rebalance when new nodes join
- `continuous` — continuously rebalance to maintain even distribution

## Data Flow

### Produce Path

1. Client sends produce request to any broker
2. Broker routes to the partition leader (may be on a different broker)
3. Leader appends to local log segment
4. Leader replicates to followers via Raft
5. Once quorum acknowledges, leader responds to client

### Fetch Path

1. Client sends fetch request to partition leader
2. Leader reads from local log segments (or page cache)
3. Leader responds with data

### Partition Movement

1. Controller decides to move partition P from broker A to broker B
2. A new replica is created on broker B as a Raft learner
3. Learner recovers the full log from the leader (this is the slow part)
4. Once caught up, learner is promoted to a full replica
5. Old replica on broker A is removed from the replica set
6. If A was the leader, leadership transfers to another replica

Movement bottlenecks:
- **Disk I/O on target**: Writing the full partition data
- **Network**: Streaming data from leader to learner
- **`raft_learner_recovery_rate`**: Rate limit on recovery throughput
- **`max_concurrent_partition_movements_per_node`**: How many movements can run in parallel per node

## Subsystem Interaction Map

```
Client Request
     │
     ▼
  Kafka API (produce/fetch)
     │
     ├──► Partition Leader (Raft)
     │        │
     │        ├──► Local Storage (log segments)
     │        └──► Replication to Followers (Raft RPC)
     │
     ▼
  Controller (metadata Raft group)
     │
     ├──► Partition Balancer (decides movements)
     ├──► Membership Manager (node join/leave)
     └──► Config Manager (cluster config changes)
```

## Resource Model

- **Disk**: Shared across all shards on a broker. Disk pressure affects all partitions on that broker.
- **Memory**: Per-shard allocation from Seastar's memory pool. OOM on one shard is a shard-level event.
- **CPU**: Per-shard. A hot partition on one shard doesn't steal CPU from other shards.
- **Network**: Shared. Both client traffic and inter-broker replication compete for bandwidth.
