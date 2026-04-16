---
name: rp-stuck-reconfiguration
description: Debug stuck partition reconfigurations and stalled partition moves
trigger: user mentions stuck reconfiguration, partition move not progressing, move stuck at X%, reconfiguration timeout, joint configuration stuck, or balancer stalled
---

# Stuck Reconfiguration Debug

You are debugging a partition reconfiguration (move) that isn't making progress. Read `knowledge/reconfiguration.md` for context on the reconfiguration state machine.

## Procedure

### Step 1: Check the event log FIRST

```bash
rpk debug event-log --since 15m
```

Look for the pattern:
- `PARTITION_MOVE start` followed by `PARTITION_MOVE stall` → move is stuck, check stall_reason
- `PARTITION_MOVE start` + `recovery finished` but NO `PARTITION_MOVE complete` → recovery done but config transition stuck (likely joint consensus issue)
- `recovery stuck` events → learner can't catch up
- `decommission.*stalled` → decommission guard fired

The event log pattern tells you where the problem is before you dig deeper.

### Step 2: Get the current move status

```bash
rpk cluster partitions move-status --print-all
```

For each stuck move, note:
- **Completion %**: Is it 0% (never started), partially done, or nearly complete?
- **Bytes moved vs partition size**: Calculate the rate if you have timestamps
- **Reconciliation statuses**: Look for error strings, high retry counts
- **From/To replicas**: Which brokers are involved?

### Step 3: Get the raft partition debug dump

This is critical for understanding WHY a move is stuck. For each stuck partition,
find the leader and fetch the debug dump:

```bash
# Find the leader broker for the stuck partition
rpk topic describe <topic> -p

# Fetch the full raft state from the leader broker
curl -s http://<leader-broker>:9644/v1/debug/partition/kafka/<topic>/<partition> | python3 -m json.tool
```

**What to look for in the debug dump:**

1. **Configuration state** (`raft_state.raft_config.configuration_state`):
   - `simple` = normal, not in transition
   - `joint` = stuck in joint consensus — this is the problem. Both old AND new quorums must agree.

2. **Followers array** — for each follower, check:
   - `match_index` vs leader's `commit_index` — how far behind is this follower?
   - `last_dirty_log_index` vs `match_index` — if dirty > match, leader thinks follower has data it hasn't confirmed
   - `is_learner: true` — this is the new replica being added. If its match_index isn't advancing, recovery is stuck.
   - `is_live: true/false` — can leader reach this follower?

3. **Key diagnostic patterns:**
   - `match_index == dirty_log_index` for all followers → recovery complete, move should finish. If it doesn't, check config state.
   - `match_index < dirty_log_index` for a learner → learner hasn't confirmed all data. Check WHY (recovery error events, throttling).
   - `match_index` is always exactly 1 behind `dirty_log_index` → possible bug in match_index update logic.
   - Config in `joint` with a node that's not in the new config still listed as voter → voter demotion failed.

4. **Compare follower state across multiple queries:**
   Run the curl twice, 10 seconds apart. If `match_index` for the stuck learner hasn't changed, recovery is truly stalled, not just slow.

Read `knowledge/raft-internals.md` for full explanation of each field.

### Step 4: Check target broker health

```bash
rpk cluster health
rpk admin brokers list
```

Is the target broker:
- Down or in maintenance mode?
- High disk usage (>85%)?
- In the "nodes down" or "high disk usage" list?

### Step 4: Check balancer status

```bash
rpk cluster partitions balancer-status
```

- **`stalled`**: Balancer can't proceed — check violations
- **Unavailable nodes**: Moves can't target these nodes
- **Over disk limit nodes**: Moves can't target these nodes
- **Current reassignments count**: How many moves are in flight?

### Step 5: Check configuration knobs

```bash
rpk cluster config get max_concurrent_partition_movements_per_node
rpk cluster config get raft_learner_recovery_rate
```

Read `knowledge/config-reference.md` for safe tuning ranges.

### Step 6: Diagnose

**0% completion, never started:**
- All movement slots may be consumed by other moves
- Target may be ineligible (disk full, decommissioning, unavailable)
- Fix: Cancel stuck moves blocking the slots, or increase concurrency

**Partially complete but stalled:**
- Learner recovery is stalling
- Check target disk/memory/network
- Check `raft_learner_recovery_rate` — may be throttled
- Fix: Increase recovery rate, address target health

**Joint configuration stuck:**
- A replica in old or new config is unreachable
- Quorum can't be achieved in both sets simultaneously
- Fix: If the unreachable node isn't coming back, force-abort the reconfiguration:
  ```bash
  curl -X POST http://<broker>:9644/v1/partitions/kafka/<topic>/<partition>/force-abort-reconfiguration
  ```
  WARNING: This is a data-safety risk. Only use when the node is permanently gone.

**Balancer stalled:**
- No eligible nodes for placement
- Check disk usage across cluster — may need to add nodes or free disk
- Check rack constraints — may be unsatisfiable with current topology

### Step 7: Suggest fixes

Based on the diagnosis, suggest one of:
1. **Increase concurrency**: `rpk cluster config set max_concurrent_partition_movements_per_node <N>`
2. **Increase recovery rate**: `rpk cluster config set raft_learner_recovery_rate <bytes>`
3. **Cancel stuck moves**: `rpk cluster partitions move-cancel`
4. **Force-abort** (dangerous): Direct admin API call
5. **Fix target health**: Address disk/memory/network on the target broker
