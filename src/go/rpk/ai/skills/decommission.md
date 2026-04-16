---
name: rp-decommission
description: Diagnose and fix slow or stuck broker decommission
trigger: user mentions decommission, draining, removing a broker, decommission is slow/stuck, or /rp-diagnose detects a decommissioning broker
---

# Decommission Diagnosis

You are diagnosing a slow or stuck broker decommission. Your goal is to identify why partition movements are slow, calculate an ETA, and suggest fixes.

## Procedure

### Step 1: Check the event log FIRST

```bash
rpk debug event-log --since 15m
```

Look for:
- `NODE_LIFECYCLE` with `decommission_start` — confirms when decommission started
- `PARTITION_MOVE` with phase `start` — moves initiated
- `PARTITION_MOVE` with phase `stall` — moves stuck (check stall_reason)
- `PARTITION_MOVE` with phase `complete` — moves finishing (or absence = stuck)
- `decommission.*stalled` — stuck_guard fired, decommission not making progress
- `recovery stuck` — learner recovery taking too long
- `balancer:` events — balancer status, failures, stalled state

The event log tells you immediately what's happening. If you see stall events, that's the bottleneck.

### Step 2: Check movement state

```bash
rpk cluster partitions balancer-status
rpk cluster partitions move-status
```

Look for:
- How many movements are in-flight vs total remaining
- Whether any movements are stalled or at 100% but not completing
- The current movement throughput (bytes/sec)

### Step 3: Identify the decommissioning broker

```bash
rpk cluster health
rpk redpanda admin brokers list
```

Identify which broker is decommissioning and how many partitions remain.

### Step 3: Check broker health and metrics

```bash
rpk cluster health
```

Scrape metrics from each broker to check resource pressure:
```bash
# For each broker address (e.g., 127.0.0.1:9644, :9645, :9646):
curl -s http://<broker>:9644/public_metrics | grep -E "redpanda_storage_disk_(free|total)_bytes|redpanda_scheduler_runtime|redpanda_rpc_request_errors"
```

Look for:
- Disk usage: `disk_free_bytes / disk_total_bytes` — if free <15% on a target broker, moves will slow or stall
- CPU: `scheduler_runtime_seconds_total` rate — if high on remaining brokers, they're overloaded with recovery traffic
- RPC errors: `rpc_request_errors_total` — inter-broker communication failures block recovery
- Under-replicated partitions on specific brokers

### Step 4: Inspect stuck partitions with raft debug dump

If moves are at 100% but not completing, or stall events show `LEARNER_RECOVERY_SLOW`,
get the raft debug dump for a stuck partition:

```bash
# Pick a stuck partition from move-status output
rpk topic describe <topic> -p  # find the leader

# Fetch the raft state
curl -s http://<leader>:9644/v1/debug/partition/kafka/<topic>/<partition> | python3 -m json.tool
```

**What to look for:**
- **Follower with `is_learner: true`** — this is the new replica being moved to
  - Compare `match_index` vs `last_dirty_log_index` — if dirty matches leader but match is behind, the leader incorrectly thinks the learner hasn't caught up
  - Compare `match_index` vs leader's `commit_index` — if close but never equal, something is preventing final catch-up
- **Configuration state** — if `joint`, check which nodes are in old vs new config
- **Run the query twice, 10s apart** — if `match_index` for the learner doesn't change, recovery is stalled. If it increases by 1 then falls back, there's a bug in the match_index update path.

### Step 5: Check relevant configuration

```bash
rpk cluster config get partition_autobalancing_mode
rpk cluster config get max_concurrent_partition_movements_per_node
rpk cluster config get raft_learner_recovery_rate
```

Read `knowledge/config-reference.md` for the impact of these settings.

### Step 5: Diagnose

Read `knowledge/partition-movement.md` to understand the movement lifecycle and failure modes.

Calculate:
- **Current rate**: bytes_moved / time elapsed
- **ETA**: remaining_bytes / current_rate
- **Bottleneck**: which resource or config is limiting throughput

Common root causes:
- **Target disk full/throttled**: `PARTITION_MOVE_STALL_REASON_TARGET_DISK_FULL` or `TARGET_DISK_THROTTLED` — target broker doesn't have enough disk space or is IO-throttled
- **Low concurrency**: `max_concurrent_partition_movements_per_node` too low — not enough parallel movements
- **Network saturation**: `NETWORK_SATURATED` stall reason — cross-rack or cross-AZ bandwidth limit
- **Learner recovery slow**: `LEARNER_RECOVERY_SLOW` — raft learner rate limit is too low
- **Memory pressure on targets**: Receiving too many partitions simultaneously

### Step 6: Suggest fixes

Based on the bottleneck, suggest one or more of these fixes (always show the exact command):

**Increase movement concurrency** (safe up to 50 on 10GbE, be cautious on 1GbE):
```bash
rpk cluster config set max_concurrent_partition_movements_per_node <value>
```

**Increase learner recovery rate** (if learner recovery is the bottleneck):
```bash
rpk cluster config set raft_learner_recovery_rate <value>
```

**Check target broker disk** — if a specific target is full, the balancer needs to pick different targets. There's no direct fix besides adding disk or removing data.

**Cancel and retry stuck movements** — if specific partitions are stuck:
```bash
rpk cluster partitions move-cancel --partition <ntp>
```

After applying a fix, re-check progress:
```bash
rpk cluster partitions move-status
```
