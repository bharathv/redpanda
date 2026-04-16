# Raft Internals — Operational Model

## Core Concepts

Each partition in Redpanda is a **Raft group** — a replicated log with a leader that handles all reads/writes and followers that replicate.

### Key Types

- **term**: Logical clock incremented at each election. Higher term always wins. Never goes backward.
- **offset**: Position in the replicated log. Tracks replication progress.
- **vnode**: `(node_id, revision)` — uniquely identifies a node incarnation across rebalancing.
- **group_id**: Identifies the Raft group (one per partition).

### State Fields (from debug dump)

| Field | Meaning |
|-------|---------|
| `term` | Current raft term — incremented at each election |
| `confirmed_term` | Term where leader confirmed authority by replicating to majority. `is_leader` is true only when `term == confirmed_term` |
| `flushed_offset` | Last durably persisted offset (fsync'd) |
| `commit_index` | Last offset committed (replicated to majority). Safe to read. |
| `majority_replicated_index` | Offset replicated to majority but not necessarily committed |
| `visibility_upper_bound_index` | Upper bound for linearizable reads |
| `last_quorum_replicated_index` | Offset replicated with full quorum ack |
| `last_snapshot_index` / `last_snapshot_term` | Most recent snapshot metadata |
| `is_leader` / `is_elected_leader` | Leadership status. `is_elected_leader` is true immediately; `is_leader` waits for `confirmed_term` |

### Follower Metrics (per replica)

| Field | Meaning |
|-------|---------|
| `match_index` | Highest offset confirmed replicated on this follower |
| `next_index` | Next offset to send to this follower |
| `committed_log_index` (flushed) | Last fsync'd offset on follower |
| `dirty_log_index` | Latest appended offset on follower (may not be flushed) |
| `last_heartbeat` | When leader last heard from this follower |
| `is_learner` | True if follower is a learner (not in quorum) |
| `is_live` | True if follower is responsive |
| `under_replicated` | True if follower is behind |

## Leader Election

### What Triggers an Election
1. **Election timeout**: Follower receives no heartbeat for `raft_heartbeat_timeout_ms`
2. **Leadership transfer**: Operator calls `rpk cluster partitions transfer-leadership`
3. **Step down**: Leader receives higher term or explicit step-down request

### Election Flow
1. Candidate increments term, votes for self
2. Sends vote requests to all voters (not learners)
3. Each voter grants vote if: candidate's log is at least as complete AND voter hasn't voted in this term
4. Candidate wins with majority of votes
5. New leader must replicate an entry in its own term to confirm leadership (`confirmed_term = term`)

### Prevote Optimization
Before a real election, candidate runs a "prevote" — tests if it can win without incrementing the term. Prevents isolated nodes from disrupting the cluster with spurious elections.

## Configuration States

Raft configurations go through states during membership changes:

```
simple → transitional → joint → simple
```

| State | Meaning |
|-------|---------|
| `simple` | Stable — single voter set, normal operation |
| `transitional` | Adding nodes — new config in progress, waiting for replication |
| `joint` | Joint consensus — both old AND new majorities must agree on commits |

### Learners

A **learner** is a replica added to the group that:
- Receives log entries from the leader
- Does NOT vote in elections
- Does NOT count toward quorum (commits don't wait for learners)
- Is promoted to **voter** once caught up

This is how safe rebalancing works: add as learner → replicate data → promote → remove old replica.

### Learner Start Offset

When adding a learner, a `learner_start_offset` can be specified so the learner doesn't need to recover the entire log from the beginning. Two policies:
- `full_local_retention`: Deliver all locally retained data
- `target_initial_retention`: Start from a calculated offset based on retention config

## Recovery

When a follower (or new learner) falls behind, recovery kicks in:

1. Leader checks if follower needs a **snapshot** (log has been truncated past follower's position)
2. If snapshot needed: transfer snapshot, then resume log replication
3. If log available: stream entries from `follower.next_index` to `leader.dirty_offset`
4. Periodic flush checkpoints (every ~1MB) during recovery
5. Final flush when learner catches up to quorum replicated index

### Recovery Scheduler Priority
- **Highest**: Controller partition (metadata)
- **High**: Internal topics (consumer groups, etc.)
- **Normal**: User topic partitions
- **Lower**: Learners (new replicas catching up)

### What Causes Recovery to Stall
1. **Log evicted past follower position** → needs snapshot, may not be available
2. **Snapshot transfer failure** → network timeout, disk full on follower
3. **Memory quota exhausted** → large batches can't be buffered
4. **Leadership changed** → recovery stops, must restart under new leader
5. **Scheduler throttle** → too many concurrent recoveries

## Raft Counters

These counters appear in metrics and debug dumps:

| Counter | What it means operationally |
|---------|----------------------------|
| `leadership_changes` | Elections completed — high count means instability |
| `recovery_requests` | Append entries sent during recovery — high means replicas catching up |
| `recovery_requests_errors` | Failed recovery attempts — stalled recoveries |
| `replicate_request_errors` | Failed replication — network issues or follower problems |
| `heartbeat_requests_errors` | Failed heartbeats — connectivity problems |
| `log_truncations` | Log evictions — normal during retention, concerning if frequent |
| `group_configuration_updates` | Membership changes completed — partition moves finishing |
| `log_flushes` | Disk syncs — high count with low throughput means small writes |
