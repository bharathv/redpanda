# Failure Patterns — Symptom to Root Cause

This document maps observable symptoms to likely root causes. When diagnosing, match what you see in `rpk debug diagnostics` and `rpk debug event-log` against these patterns.

## Pattern: Under-replicated partitions climbing on one broker

**Symptoms**:
- `under_replicated_partitions > 0` on one broker in diagnostics
- `ReplicaSetChangeEvent` with type `SHRINK` in event log for partitions on that broker

**Likely causes**:
1. **Disk failing**: Check `disk_write_latency_p99_ms` on that broker. If >100ms (normal <20ms), disk is degrading. Confirm with `DiskFailureEvent` in event log.
2. **Network partition**: Check `rpc_latency_p99_ms` to that broker from others. High or timeout → network issue. Confirm with `RpcTimeoutCascadeEvent`.
3. **Broker overloaded**: Check `cpu_pct` and `memory_pct`. If both high, broker can't keep up with replication.

## Pattern: Leaderless partitions appearing

**Symptoms**:
- `leaderless > 0` in cluster diagnostics
- `rpk cluster health` reports leaderless partitions

**Likely causes**:
1. **Broker down**: Check `NodeLifecycleEvent` with phase `DOWN_DETECTED`. If a broker went down, its leader partitions become leaderless until new leaders are elected.
2. **Split brain / network partition**: All brokers up but can't communicate. Check `RpcTimeoutCascadeEvent` across multiple brokers.
3. **All replicas down**: If a partition's entire replica set is on brokers that are down (possible with RF=1 or small clusters).

**Resolution**: Leaderless partitions usually self-resolve once the underlying issue (broker down, network) is fixed and Raft elects new leaders. If they persist:
- Check if the broker is actually reachable
- Check if the Raft group has quorum (majority of replicas must be available)

## Pattern: Sudden produce/fetch latency spike

**Symptoms**:
- Users report slow produces or fetches
- `produce_throughput_bytes_sec` or `fetch_throughput_bytes_sec` dropped on one or more brokers

**Likely causes**:
1. **Disk pressure**: `ResourcePressureEvent` with `RESOURCE_TYPE_DISK`. Check `disk_write_latency_p99_ms`.
2. **Compaction storm**: `ImpactedSubsystem` is `COMPACTION`. Heavy compaction steals disk I/O from produce/fetch.
3. **Leadership transfer storm**: Many `LeadershipTransferEvent` entries in short succession. Causes brief unavailability per partition during transfer.
4. **Large partition movement**: Active movements compete for disk and network. Check `partition_move` events.

## Pattern: Consumer lag growing across all partitions

**Symptoms**:
- `rpk group describe` shows lag increasing on all partitions simultaneously
- Produce throughput is normal

**Likely causes**:
1. **Consumer application issue**: Not a broker problem. Consumer is processing slowly (DB bottleneck, external API timeout, GC pause).
2. **Fetch throttled by broker**: Check `resource_pressure` on the leader brokers for the affected topic. If brokers are under pressure, fetch throughput drops.
3. **Consumer rebalance**: Consumers are repeatedly joining/leaving the group. During rebalance, no consumption happens.

## Pattern: Consumer lag growing on specific partitions only

**Symptoms**:
- Lag concentrated on a small number of partitions
- Other partitions of the same topic are fine

**Likely causes**:
1. **Hot partition**: Skewed key distribution sends disproportionate traffic to those partitions.
2. **Leader broker for those partitions is struggling**: Check the leader broker's health in diagnostics.
3. **Partition movement in progress**: A partition being moved may have brief fetch disruptions during leadership transfer.

## Pattern: Cluster healthy but balancer status is "stalled"

**Symptoms**:
- `rpk cluster health` says healthy
- `rpk cluster partitions balancer-status` shows `stalled`

**Likely causes**:
1. **Allocation failures**: The balancer wants to move partitions but can't find valid targets (all nodes full, rack constraints unsatisfiable). Check `AllocationFailureEvent` in event log.
2. **Stuck movements blocking new ones**: Existing movements are stalled, consuming all movement slots. New movements can't start. Fix the stalled movements first.

## Pattern: Post-maintenance partition imbalance

**Symptoms**:
- A broker recently exited maintenance mode
- It has fewer partitions/leaders than other brokers
- Balancer status is `ready` (thinks distribution is fine)

**Likely causes**:
1. **Balancer tolerance**: The balancer has a tolerance threshold — small imbalances don't trigger rebalancing. The post-maintenance skew is within tolerance.
2. **Leadership preference not restored**: Partitions moved off during maintenance but leadership didn't transfer back. Run `rpk cluster partitions balance` to trigger explicit rebalancing.
