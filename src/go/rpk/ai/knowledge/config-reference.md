# Configuration Reference — Diagnostically Relevant Settings

This document covers cluster configuration knobs that are most relevant during operational diagnosis. For each setting: what it controls, default value, safe tuning ranges, and side effects.

## Partition Movement

### `max_concurrent_partition_movements_per_node`
- **What**: Maximum number of partitions that can simultaneously move to or from a single node.
- **Default**: 10
- **Tuning**:
  - 10-20: Conservative, safe for all network configs
  - 20-50: Aggressive, good for 10GbE+ networks
  - 50-100: Very aggressive, use only with fast storage and network. Monitor for resource pressure.
  - >100: Not recommended. Likely to cause resource exhaustion on target brokers.
- **Side effects**: Higher values speed up decommission/rebalance but increase disk I/O, memory, and network pressure on the brokers involved.

### `raft_learner_recovery_rate`
- **What**: Rate limit (bytes/sec) for streaming data from leader to a recovering learner (new replica).
- **Default**: Unlimited (0)
- **Tuning**: Set this if learner recovery is saturating network or disk. A reasonable starting point is 100MB/s per node.
- **Side effects**: Lowering this slows down partition movements but reduces impact on production traffic.

### `partition_autobalancing_mode`
- **What**: When the partition balancer runs.
- **Values**:
  - `off`: No automatic rebalancing. Operator must trigger manually.
  - `node_add`: Rebalance only when new nodes join the cluster.
  - `continuous`: Continuously evaluate and rebalance.
- **Default**: `node_add`
- **Side effects**: `continuous` mode creates ongoing partition movements which consume resources. Use in production only if you accept the overhead.

## Resource Management

### `storage_min_free_bytes`
- **What**: Minimum free disk space (bytes) before Redpanda starts rejecting writes.
- **Side effects**: When disk reaches this threshold, produce requests are rejected. This is a safety mechanism — do not lower it.

### `storage_target_free_bytes`
- **What**: Target free disk space. Redpanda tries to keep at least this much free via log cleanup.
- **Tuning**: Should be higher than `storage_min_free_bytes`. Leave enough headroom for partition movements (incoming data).

## Replication

### `default_topic_replications`
- **What**: Default replication factor for new topics.
- **Default**: 1 (single node), 3 (multi-node)
- **Impact**: RF=1 means a single broker failure loses that partition's availability. RF=3 tolerates 1 broker failure.

### `raft_heartbeat_interval_ms`
- **What**: How often Raft leaders send heartbeats to followers.
- **Default**: 150ms
- **Impact**: Lower = faster failure detection but more network overhead. Higher = slower detection. Don't change this without understanding Raft timing.

### `raft_heartbeat_timeout_ms`
- **What**: How long a follower waits without a heartbeat before triggering a leader election.
- **Impact**: Must be > `raft_heartbeat_interval_ms`. Too low = spurious elections under load. Too high = slow failure detection.

## Performance

### `fetch_max_bytes`
- **What**: Maximum bytes returned in a single fetch response.
- **Impact**: Higher values improve throughput for large consumers but increase memory usage per fetch.

### `kafka_batch_max_bytes`
- **What**: Maximum size of a single produce batch.
- **Impact**: Larger batches improve throughput but increase latency per batch.

## Diagnostic Settings

### `enable_diagnostic_events`
- **What**: Whether subsystems emit diagnostic events to the per-shard event log.
- **Default**: true
- **Impact**: Disabling this removes the event log data that `rpk debug event-log` reads. Only disable if event emission causes measurable overhead (unlikely — it's best-effort and non-blocking).

### `diagnostic_event_log_max_size_bytes`
- **What**: Maximum size of the per-shard diagnostic event log file before rotation.
- **Default**: 10MB per shard
- **Impact**: Larger = more history but more disk usage. At 10MB per shard on a 16-core machine, that's 160MB total per broker.
