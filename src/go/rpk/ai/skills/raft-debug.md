---
name: rp-raft-debug
description: Inspect raft partition state and debug raft-level issues
trigger: user asks about raft state, partition state dump, follower lag, leader election issues, replication lag, or wants to inspect a specific partition's internal state
---

# Raft Partition Debug

You are inspecting the internal raft state of a partition. Read `knowledge/raft-internals.md` for context on what each field means.

## Procedure

### Step 1: Get the partition debug dump

Ask the user which partition they want to inspect (namespace/topic/partition). Then fetch the debug state:

```bash
curl -s http://<leader-broker>:9644/v1/debug/partition/{namespace}/{topic}/{partition} | python3 -m json.tool
```

If unsure which broker is the leader:
```bash
rpk topic describe <topic> -p
```
The leader column shows which broker to query.

### Step 2: Check leadership status

From the debug dump, check:
- `is_leader` and `is_elected_leader` — if `is_elected_leader` is true but `is_leader` is false, the leader hasn't confirmed authority yet (hasn't replicated to majority in this term)
- `term` vs `confirmed_term` — should match on a stable leader
- If neither is true, the partition is leaderless — check follower states for why no election is succeeding

### Step 3: Check follower health

From the `followers` array in the debug dump:
- **`is_live`**: false means the leader can't reach this follower
- **`under_replicated`**: true means follower is behind
- **`match_index` vs leader's `commit_index`**: Gap shows how far behind the follower is
- **`is_learner`**: true means this is a new replica still catching up (not in quorum)
- **`last_heartbeat`**: How recently the follower responded

### Step 4: Check configuration state

Look at the raft configuration in the dump:
- **`configuration_state: simple`**: Normal — single voter set
- **`configuration_state: joint`**: Joint consensus — both old and new quorums needed. This happens during reconfiguration.
- **Voters vs learners**: Learners don't vote. If a quorum of voters is unreachable, the group can't make progress.

### Step 5: Check recovery status

```bash
curl -s http://<broker>:9644/v1/raft/recovery_status | python3 -m json.tool
```

This shows per-shard recovery state: how many partitions are recovering, pending, and queued.

### Step 6: Diagnose

Read `knowledge/raft-internals.md` for interpretation guidance.

**Common issues:**
- **Leaderless partition**: No voter can win an election. Check if majority of voters are reachable. Check if terms are diverging (split brain indicator).
- **Under-replicated follower**: Follower is alive but behind. Check `match_index` gap. If large, recovery may be slow. Check disk/network on the follower.
- **Stuck learner**: `is_learner: true` and `match_index` not advancing. Recovery stalled — check recovery_status and target broker health.
- **Match index anomaly**: If a learner's `last_dirty_log_index` matches the leader's dirty offset (data fully transferred) but `match_index` is behind (leader thinks it hasn't caught up), this is a bug in the match_index update logic. The leader is not correctly acknowledging the follower's progress. Look at `successfull_append_entries_reply()` in `src/v/raft/consensus.cc` for where `match_index` is set.
- **Configuration stuck in joint**: Check which replicas are in the old vs new config. If a replica in either set is down, quorum can't be achieved in both sets. Also check if a learner should have been promoted to voter but wasn't (match_index anomaly can cause this).
- **Leader not confirmed**: `is_elected_leader` true but `is_leader` false. Leader can't replicate to majority — likely quorum issue.
- **Infinite recovery loop**: Recovery "finishes" but immediately restarts because `needs_recovery()` check sees `match_index < dirty_offset`. Check if match_index is being decremented or not updated correctly.

### Step 7: Check raft leader table

```bash
curl -s http://<broker>:9644/v1/raft/leader_table | python3 -m json.tool | head -50
```

Shows leader assignments across all partitions on this broker. Useful for identifying leadership imbalance or many leaderless partitions.
