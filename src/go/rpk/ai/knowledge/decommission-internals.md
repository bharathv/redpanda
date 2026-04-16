# Decommission Internals — Operational Model

## How Decommission Works

Decommissioning a broker removes it from the cluster by moving all its partitions to other brokers. The flow:

1. **Operator initiates**: `rpk admin brokers decommission <id>`
2. **Members manager** marks the node as `decommissioned` in the allocator
3. **Partition balancer** detects the decommissioning node and plans moves
4. **Each tick**, the balancer calls `get_node_drain_actions()` which:
   - Iterates all partitions with replicas on the decommissioning node
   - For each partition: plans a `move_replica()` to a healthy node
   - Applies hard constraints: never move TO another decommissioning node, never overflow disk
   - Applies soft constraints: prefer balanced distribution, least-filled disks, rack awareness
5. **Moves execute** via the normal reconfiguration state machine
6. Once `replicas_left == 0`, balancer calls `finish_node_reallocations`
7. Node transitions from "decommissioning" to fully removed

## Balancer Rules During Decommission

### Hard Constraints (must satisfy)
- `distinct_from(decommissioning_nodes)` — never allocate new replicas to decommissioning nodes
- `distinct_from(unavailable_nodes)` — never allocate to timed-out nodes
- `disk_not_overflowed_by_partition()` — target disk must not exceed `max_disk_usage_ratio`

### Soft Constraints (best effort, in priority order)
1. **Topic-aware balance**: Spread replicas of the same topic evenly
2. **Global balance**: Balance total replica counts across brokers
3. **Least disk filled**: Prefer nodes with more free disk

### Concurrent Move Limit
- `max_concurrent_partition_movements_per_node` controls how many moves run in parallel per node
- Both source and target count — decommissioning a node with 5,000 partitions at concurrency 10 means ~500 rounds of moves
- Increasing this speeds up decommission but increases resource pressure

## What Causes Slow Decommission

1. **Low concurrency**: Default `max_concurrent_partition_movements_per_node = 10`. With 5,000 partitions, that's 500 rounds at ~30s each = ~4 hours.

2. **Target broker disk full**: If target brokers are near `max_disk_usage_ratio`, the balancer can't place replicas → moves stall → `stalled` status.

3. **Large partitions**: A 100GB partition takes much longer to move than a 1GB one. A few large partitions can dominate the decommission time.

4. **Raft learner recovery rate**: If `raft_learner_recovery_rate` is set low, data transfer to the new replica is throttled.

5. **Rack constraints**: If the decommissioning node is the only node in its rack, rack-aware placement may limit where replicas can go.

6. **Already-stalled moves**: If existing reconfigurations are stuck (from before the decommission), they consume movement slots and block new moves.

## Reallocation Failure Reasons

When the balancer can't move a partition off the decommissioning node:

| Failure | Meaning |
|---------|---------|
| `no_eligible_node_found` | No healthy node has enough disk/capacity for this partition |
| `over_partition_fd_limit` | Target node hit file descriptor limit |
| `over_partition_memory_limit` | Target node hit memory limit |
| `over_partition_core_limit` | Target node hit core limit |
| `no_quorum` | Not enough replicas for quorum after move |
| `reconfiguration_in_progress` | Partition already being moved — can't start another |
| `partition_disabled` | Partition is disabled — can't be moved |

## Monitoring Decommission Progress

```bash
# Overall status
rpk admin brokers decommission-status <broker-id>

# Partition movement progress
rpk cluster partitions move-status

# Balancer status — check for stalls
rpk cluster partitions balancer-status

# How many partitions remain
rpk cluster partitions list | grep <broker-id>
```

## Recommission (Undo Decommission)

If decommission hasn't finished yet:
```bash
rpk admin brokers recommission <broker-id>
```
This reverses the decommission — the balancer stops draining the node and treats it as a normal member again.

## Auto-Decommission

If `node_autodecommission_timeout` is configured, a node that's unresponsive for longer than this timeout can be automatically decommissioned. Requirements:
- A quorum of cluster members must agree the node is unresponsive
- No other node is already being decommissioned
- The node is not in maintenance mode
