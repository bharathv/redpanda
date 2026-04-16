---
name: rp-partition-balance
description: Diagnose and fix partition imbalance or slow partition movement
trigger: user mentions partition imbalance, skew, slow rebalance, uneven distribution, hot brokers, or partition movement issues
---

# Partition Balance Diagnosis

You are diagnosing partition imbalance or slow partition movement. Your goal is to quantify the skew, identify the bottleneck, and guide rebalancing.

## Procedure

### Step 1: Get partition distribution

```bash
rpk debug diagnostics
```

Compare `partitions` and `leader_partitions` across brokers. Calculate:
- **Partition skew**: (max - min) / average * 100%
- **Leader skew**: same for leader_partitions
- Skew > 20% is worth investigating. Skew > 50% is a problem.

### Step 2: Check balancer state

```bash
rpk cluster partitions balancer-status
```

Look at the balancer status:
- `off` — balancer is disabled, no automatic rebalancing
- `ready` — balancer sees no work to do (it may disagree with your skew assessment — check constraints)
- `in_progress` — rebalancing is active, check movement progress
- `stalled` — rebalancing is stuck, need to investigate

### Step 3: Check active movements

```bash
rpk cluster partitions move-status
```

If movements are in-flight, check throughput and whether any are stalled.

### Step 4: Check cluster health

```bash
rpk cluster health
rpk admin brokers list
```

Look for:
- Nodes down or in maintenance mode
- High disk usage nodes that can't accept more partitions
- Under-replicated partitions indicating broker issues

### Step 5: Diagnose

Read `knowledge/partition-movement.md` for context on how the balancer decides what to move.

Common root causes for imbalance:
- **Balancer disabled**: `partition_autobalancing_mode` set to `off`
- **Recent node addition**: New node has no partitions yet, balancer needs time to catch up
- **Rack constraints**: Rack-aware placement limits where partitions can go
- **Topic-level RF mismatch**: Some topics have RF=1, can't spread across all brokers
- **Maintenance mode aftermath**: Partitions moved off a node for maintenance and haven't moved back

Common root causes for slow movement:
- **Low concurrency**: Check `max_concurrent_partition_movements_per_node`
- **Large partitions**: A few very large partitions dominate movement time
- **Resource pressure on targets**: Check disk/memory/CPU on receiving brokers

### Step 6: Suggest fixes

**Enable balancer** (if off):
```bash
rpk cluster config set partition_autobalancing_mode continuous
```

**Increase movement concurrency**:
```bash
rpk cluster config set max_concurrent_partition_movements_per_node <value>
```

**Trigger manual rebalance** (if balancer is `ready` but skew exists):
```bash
rpk cluster partitions balance
```

After applying fixes, monitor progress:
```bash
rpk cluster partitions move-status
rpk cluster partitions balancer-status
```
