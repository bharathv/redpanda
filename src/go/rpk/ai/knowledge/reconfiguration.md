# Partition Reconfiguration — Operational Model

## What is a Reconfiguration

A **reconfiguration** is a change to a partition's replica set — adding, removing, or moving replicas across brokers. This is the fundamental primitive behind partition balancing, decommission, and manual partition moves.

## Reconfiguration States

```
in_progress → [completed]
            → cancelled → [reverted]
            → force_update → [completed]
            → force_cancelled → [reverted]
```

| State | Meaning |
|-------|---------|
| `in_progress` | Normal move — learner added on target, recovering, will promote when caught up |
| `cancelled` | Graceful cancel — reverting the move, old replicas restored |
| `force_update` | Force move — bypasses normal reconciliation. Used for lost-quorum recovery |
| `force_cancelled` | Emergency abort of a force move |

## How a Reconfiguration Works (Normal Flow)

1. **Controller decides to move** partition P from broker A to broker B
2. **Topic table** creates an `in_progress_update` with previous and target replica sets
3. **Controller backend** on each broker runs `reconcile_partition_reconfiguration()`:
   - Checks if configuration is already reconciled → if yes, try to finish
   - If not reconciled, dispatches the raft configuration update
4. **Raft** adds broker B as a **learner** in the group configuration
5. **Recovery** streams data from leader to learner B
6. Once caught up, raft promotes learner to **voter** (joint consensus)
7. Once joint consensus resolves, old replica on A is removed
8. Controller backend calls `dispatch_update_finished()` to mark complete

## How Reconfigurations Get Stuck

### `waiting_for_recovery`
- Raft configuration update was dispatched but replicas haven't fully applied it
- The learner is still recovering (catching up with the leader's log)
- **Common cause**: Large partition, slow network, disk pressure on target
- **Diagnostic**: Check `rpk cluster partitions move-status` — look at bytes moved vs partition size. If progress is stalling, check target broker health.

### `waiting_for_reconfiguration_finish`
- Configuration is reconciled but another node must finish the update
- Current node is not the designated finisher (usually the leader)
- **Usually resolves itself** — just needs the leader to acknowledge

### `partition_configuration_in_joint_mode`
- Configuration is stuck in JOINT state — both old and new quorums required
- JOINT → SIMPLE transition hasn't completed
- **Common cause**: One of the replicas in the old or new set is unavailable, preventing quorum in both sets
- **Diagnostic**: Check which replicas are in the configuration and whether they're all healthy

### `partition_configuration_leader_config_not_committed`
- Leader can't commit the configuration change to majority
- **Common cause**: Network partition, too many replicas down
- **Diagnostic**: Check if majority of replicas are reachable

### `partition_configuration_revision_not_updated`
- Replicas haven't applied the latest configuration yet
- **Common cause**: Slow raft replication, leader election in progress

## Reconfiguration Policy

Controls how much data the learner needs to recover:

| Policy | Behavior |
|--------|----------|
| `full_local_retention` | Learner gets all locally retained data (default) |
| `target_initial_retention` | Learner starts from a calculated offset, less data transfer |

## Admin API Endpoints

### View ongoing reconfigurations
```bash
rpk cluster partitions move-status
# Calls GET /v1/cluster/partitions/reconfigurations
# Shows: partition, from/to replicas, bytes moved/remaining, completion %
# With --print-all: shows per-node reconciliation status, retry counts, errors
```

### Cancel a specific reconfiguration
```bash
rpk cluster partitions move-cancel --partition <topic>/<partition>
# POST /v1/cluster/partitions/{ns}/{topic}/{partition}/cancel-reconfiguration
```

### Cancel all reconfigurations
```bash
rpk cluster partitions move-cancel
# POST /v1/cluster/cancel_reconfigurations
```

### Force-abort a stuck reconfiguration (dangerous)
```bash
# No rpk command — direct admin API call:
curl -X POST http://<broker>:9644/v1/partitions/{ns}/{topic}/{partition}/force-abort-reconfiguration
# WARNING: Unclean abort. May lose data if learner was the only copy.
```

## Debugging a Stuck Reconfiguration

1. **Check move status**: `rpk cluster partitions move-status --print-all`
   - Look at completion percentage — is it advancing?
   - Check reconciliation statuses per node — what errors are reported?
   - Check retry counts — high retries suggest persistent failure

2. **Check partition debug state**: 
   ```bash
   curl http://<leader-broker>:9644/v1/partitions/{ns}/{topic}/{partition}/debug
   ```
   - Look at follower metrics — is the learner's `match_index` advancing?
   - Check `is_live` for each follower
   - Check if configuration is in `joint` state

3. **Check target broker health**:
   - Disk full? Memory pressure? Network issues?
   - If target is unhealthy, the learner can't make progress

4. **Check balancer status**: `rpk cluster partitions balancer-status`
   - If `stalled` — balancer can't find valid targets for moves
   - Check violations: unavailable nodes, disk full nodes

5. **Last resort**: Force-abort the reconfiguration and let the balancer retry
