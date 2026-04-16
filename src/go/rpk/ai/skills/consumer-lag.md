---
name: rp-consumer-lag
description: Diagnose consumer group lag issues
trigger: user mentions consumer lag, slow consumers, stuck offsets, consumer group issues, consumers falling behind, or consumer rebalance problems
---

# Consumer Lag Diagnosis

You are diagnosing consumer group lag. Your goal is to determine whether the root cause is on the producer side, consumer side, or broker side, and suggest targeted fixes.

## Procedure

### Step 1: Get consumer group state

Ask the user for the consumer group name if not provided, then:

```bash
rpk group describe <group-name>
```

Look for:
- **Total lag across all partitions** — is it growing, stable, or shrinking?
- **Per-partition lag distribution** — is lag concentrated on specific partitions (hot partitions) or evenly spread?
- **Member assignments** — are all consumers assigned partitions? Are assignments balanced?
- **Empty members** — consumers assigned but with no partitions indicate a rebalance issue

### Step 2: Get topic details

```bash
rpk topic describe <topic-name>
```

Check:
- Partition count vs consumer count — are there more partitions than consumers (normal) or fewer (some consumers idle)?
- Replication factor — under-replicated partitions may have slow fetches

### Step 3: Check broker health for affected partitions

```bash
rpk cluster health
rpk admin brokers list
```

For partitions with the highest lag, identify which brokers are leaders for those partitions. Check those brokers for:
- Are they in the nodes down list?
- Are they flagged for high disk usage?
- Are there under-replicated partitions on those brokers?

### Step 4: Check partition state

```bash
rpk cluster partitions balancer-status
rpk cluster partitions move-status
```

Look for:
- Active partition movements on leader brokers — can cause brief fetch disruptions
- Balancer stalled — may indicate resource pressure

### Step 5: Diagnose

Determine the root cause category:

**Producer spike**: Lag appeared suddenly across all partitions simultaneously. Produce throughput jumped. Consumers are processing at their normal rate but can't keep up with the burst. This is transient — lag will recover if produce rate returns to normal.

**Consumer slowdown**: Lag is growing steadily. Produce rate is normal. Consumers are processing slower than usual. Causes: application-side bottleneck (DB, external API), GC pauses, consumer rebalancing loop.

**Hot partitions**: Lag is concentrated on a few partitions. Usually caused by skewed key distribution sending disproportionate traffic to specific partitions.

**Broker issue**: Lag correlates with broker health — high disk latency, leadership transfers, or resource pressure on the leader broker for lagging partitions.

**Consumer rebalance storm**: Consumers are repeatedly joining and leaving the group. Look for rapidly changing member assignments in `rpk group describe` output.

### Step 6: Suggest fixes

**For producer spikes**: No broker-side fix needed. Monitor and wait for recovery. If sustained, suggest scaling consumers.

**For consumer slowdown**: This is application-side — suggest the user check their consumer application. On the broker side, ensure fetch throughput isn't bottlenecked.

**For hot partitions**: Suggest reviewing the partitioning strategy (key distribution). If repartitioning isn't feasible:
```bash
rpk topic alter-config <topic> --set num.partitions=<higher-count>
```

**For broker issues**: Refer to the relevant broker-side diagnosis (disk, network, etc.).

**For rebalance storms**: Check consumer group session timeout and heartbeat interval configuration. These are client-side settings, not broker-side.
