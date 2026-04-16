---
name: rp-controller-debug
description: Debug controller partition issues, metadata problems, and cluster-level state
trigger: user mentions controller, metadata issues, cluster state problems, can't create topics, or configuration not propagating
---

# Controller Debug

The **controller** is a special Raft group (partition 0 of the internal `controller` topic) that manages all cluster metadata: partition assignments, broker membership, configuration changes, and partition movements.

## Procedure

### Step 1: Identify the controller leader

```bash
rpk cluster health
```

The `Controller ID` field shows which broker is the controller leader. All metadata operations must go through this broker.

### Step 2: Check controller partition health

```bash
curl -s http://<controller-leader>:9644/v1/partitions/redpanda/controller/0/debug | python3 -m json.tool
```

Check:
- **`is_leader`**: Should be true on the controller leader
- **`commit_index`**: Should be advancing. If frozen, controller can't make progress.
- **Follower states**: All brokers should be voters with healthy `match_index`
- **Configuration state**: Should be `simple` normally

### Step 3: Check raft recovery status

```bash
# On each broker
curl -s http://<broker>:9644/v1/raft/recovery_status | python3 -m json.tool
```

If the controller partition is recovering on a broker, it blocks metadata operations until caught up.

### Step 4: Check for reconfigurations affecting controller

```bash
rpk cluster partitions move-status
```

If the controller partition itself is being moved (rare but possible), metadata operations stall until the move completes.

### Step 5: Check partition leader table

```bash
curl -s http://<broker>:9644/v1/raft/leader_table | python3 -m json.tool
```

The leader table shows which broker is leader for each partition. If many partitions show leader=-1, there's a cluster-wide leadership issue.

### Step 6: Diagnose

**Controller leader keeps changing:**
- Network instability between brokers — check heartbeat timeout
- One broker is slow (disk pressure, CPU) — can't respond to heartbeats in time
- Check `leadership_changes` counter — high count confirms instability

**Metadata operations timing out:**
- Controller can't commit to majority — check if majority of brokers are reachable
- Controller partition stuck in recovery — wait for recovery to complete
- Controller partition being reconfigured — wait for move to complete

**Topics/partitions not being created:**
- Controller may be leaderless — check `rpk cluster health`
- Controller backlog — many pending operations
- Check controller broker logs for errors

**Configuration changes not propagating:**
- Each broker applies config changes from the controller log
- If a broker is behind on the controller log, it has stale config
- Check follower `match_index` for the controller partition on each broker
