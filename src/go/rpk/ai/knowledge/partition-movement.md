# Partition Movement — Operational Model

## Lifecycle

A partition movement goes through these phases:

```
PLANNED → START → PROGRESS → COMPLETE
                      │
                      └──► STALL ──► (recovery or CANCEL)
```

### Phases

1. **Planned**: Controller has decided to move this partition. Waiting for a movement slot (bounded by `max_concurrent_partition_movements_per_node`).

2. **Start**: Movement initiated. A new Raft learner is created on the target broker. The target starts recovering the partition log from the leader.

3. **Progress**: Data is being streamed from the leader to the learner. The `progress_pct` field in the event log tracks how far along the recovery is. Movement speed depends on:
   - Partition size (larger = longer)
   - `raft_learner_recovery_rate` (throttle on recovery throughput)
   - Disk I/O on both source and target
   - Network bandwidth between source and target

4. **Stall**: Movement has stopped making progress. The event log records a `PartitionMoveStallReason`:
   - `TARGET_DISK_FULL`: Target broker has no disk space for the new replica
   - `TARGET_DISK_THROTTLED`: Target broker's disk is under pressure, writes are slow
   - `SOURCE_UNAVAILABLE`: Source broker (usually the leader) went down
   - `NETWORK_SATURATED`: Network path between source and target is congested
   - `MEMORY_PRESSURE`: Target broker is low on memory, can't allocate buffers
   - `CROSS_RACK_BANDWIDTH`: Cross-rack or cross-AZ link is the bottleneck
   - `LEARNER_RECOVERY_SLOW`: Recovery is progressing but below expected rate

5. **Complete**: Learner is fully caught up. The old replica is removed, replica set is updated.

6. **Cancel**: Movement was cancelled (operator-initiated or by the controller due to a conflict).

## Key Configuration

| Config | Default | Impact |
|--------|---------|--------|
| `max_concurrent_partition_movements_per_node` | 10 | How many partitions can move to/from a single node simultaneously. Higher = faster decommission, but more resource pressure on that node. |
| `raft_learner_recovery_rate` | — | Bytes/sec rate limit on learner recovery. Limits how fast data streams from leader to learner. |
| `partition_autobalancing_mode` | `node_add` | When the balancer triggers: `off`, `node_add`, or `continuous`. |

## Common Failure Patterns

### Slow decommission

**Signature**: Broker in `DECOMMISSIONING` state for hours. Many `PARTITION_MOVE_PHASE_PROGRESS` events but `progress_pct` advancing slowly.

**Diagnosis**:
1. Check `max_concurrent_partition_movements_per_node` — if low (default 10), few partitions move at a time
2. Check target broker disk — `disk_usage_pct > 85%` on targets causes throttling
3. Check partition sizes — a few very large partitions (100GB+) dominate movement time
4. Check `raft_learner_recovery_rate` — if set low, recovery throughput is capped

### Stuck movements

**Signature**: `PARTITION_MOVE_PHASE_STALL` events, movement not progressing for >10 minutes.

**Diagnosis**:
1. Read the `stall_reason` enum — it tells you exactly what's wrong
2. `TARGET_DISK_FULL` → target needs more disk or the balancer needs to pick a different target
3. `SOURCE_UNAVAILABLE` → the leader went down, movement can't proceed until a new leader is elected and recovery restarts
4. `NETWORK_SATURATED` → reduce concurrent movements or increase network capacity

### Movement cascade

**Signature**: Decommissioning one broker causes resource pressure on remaining brokers, which causes more movements, which causes more pressure.

**Diagnosis**:
1. Check `resource_pressure` events on target brokers — are they appearing after movements started?
2. Reduce concurrent movements to lower pressure
3. Consider decommissioning more slowly (lower concurrency) even though it takes longer
