---
name: rp-balancer-debug
description: Debug the partition balancer, understand why partitions aren't moving, stalled balancer
trigger: user mentions partition balancer, why aren't partitions moving, balancer stalled, uneven partition distribution, rack constraint issues, disk full preventing moves
---

# Partition Balancer Debug

The partition balancer runs on the controller leader and continuously evaluates partition distribution. Read `knowledge/partition-movement.md` and `knowledge/decommission-internals.md` for context.

## Procedure

### Step 1: Check balancer status

```bash
rpk cluster partitions balancer-status
```

Interpret the status:
- **`off`**: Balancer disabled. Check `partition_autobalancing_mode` config.
- **`starting`**: Controller leader hasn't completed first tick yet. Wait.
- **`ready`**: No work to do — distribution is within tolerance.
- **`in_progress`**: Actively moving partitions. Check move-status for details.
- **`stalled`**: Balancer wants to move partitions but can't — allocation failures.

### Step 2: Check violations

From the balancer-status output:
- **Unavailable nodes**: Nodes the balancer can't reach. Partitions on these nodes need to move but the balancer can't create learners on them.
- **Over disk limit nodes**: Nodes exceeding `max_disk_usage_ratio`. The balancer won't place new replicas on these nodes.

### Step 3: Check current moves

```bash
rpk cluster partitions move-status
```

- How many moves are in flight?
- Are any stuck? (0% progress for a long time)
- Are moves completing? Check completion percentages.

### Step 4: Check partition distribution

```bash
rpk cluster partitions list
```

Look at the replica distribution across brokers. Is it heavily skewed?

### Step 5: Check relevant config

```bash
rpk cluster config get partition_autobalancing_mode
rpk cluster config get max_concurrent_partition_movements_per_node
rpk cluster config get partition_autobalancing_max_disk_usage_ratio
```

### Step 6: Diagnose

**Balancer is `ready` but distribution is uneven:**
- The imbalance is within the balancer's tolerance threshold
- May happen after maintenance mode — partitions moved off but didn't move back
- Fix: Trigger manual rebalance: `rpk cluster partitions balance`

**Balancer is `stalled`:**
- Check violation lists — unavailable or full nodes
- Allocation failures — no valid targets for moves
- Common causes:
  - All remaining nodes are near disk limit
  - Rack constraints unsatisfiable (e.g., 2-rack cluster with RF=3)
  - Too many concurrent moves consuming all slots
- Fix: Add disk, add nodes, relax rack constraints, or cancel stuck moves

**Balancer is `in_progress` but moves are slow:**
- Check `max_concurrent_partition_movements_per_node` — may be too low
- Check `raft_learner_recovery_rate` — may be throttled
- Check target broker health — disk IO, memory pressure
- Large partitions dominate move time
- Fix: Increase concurrency, increase recovery rate, address broker health

**Balancer is `off`:**
```bash
rpk cluster config set partition_autobalancing_mode continuous
```
Or `node_add` if you only want rebalancing when nodes join.

### Step 7: Understand the balancer's decision logic

The balancer plans moves in this priority order:
1. **Drain decommissioning nodes** — move all partitions off
2. **Auto-decommission** unresponsive nodes (if configured)
3. **Repair rack constraints** — fix partitions with too many replicas in one rack
4. **Empty full nodes** — move smallest partitions off disk-full nodes
5. **Count rebalancing** — even out partition counts across brokers (continuous mode only)

Each phase respects the concurrent move limit. If phase 1 (decommission) consumes all slots, phases 2-5 won't run until slots free up.
