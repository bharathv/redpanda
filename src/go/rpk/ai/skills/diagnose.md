---
name: rp-diagnose
description: General cluster health triage — first skill to run for any issue
trigger: user asks about cluster health, general issues, "what's wrong", something seems off, or as a starting point for any diagnosis
---

# Cluster Health Triage

You are performing a general cluster health triage. Your goal is to identify the top issues, correlate symptoms with recent events, and direct the user to a more specific skill if needed.

## Procedure

### Step 1: Gather cluster state

Run these commands to get the current cluster snapshot:

```bash
rpk cluster health
rpk admin brokers list
rpk cluster partitions balancer-status
rpk cluster partitions move-status
```

### Step 2: Gather metrics from each broker

Scrape Prometheus metrics to get resource state:
```bash
# For each broker (check rpk profile for addresses):
curl -s http://<broker>:9644/public_metrics | grep -E "redpanda_storage_disk|redpanda_scheduler_runtime|redpanda_rpc_request_errors|memory_(allocated|free)_memory|redpanda_kafka_under_replicated"
```

Compare across brokers — look for one broker with much higher CPU, disk, or errors than others.

### Step 3: Gather topic and config state

```bash
rpk topic list
rpk cluster config export 2>&1 | head -50
```

### Step 4: Correlate and diagnose

Read `knowledge/failure-patterns.md` to match what you see against known failure signatures.

Cross-reference the outputs:
- Under-replicated partitions + specific brokers down → check those brokers
- Leaderless partitions + nodes down → identify which node is missing
- Active movements + balancer stalled → identify the bottleneck
- High disk usage in metrics + stall events → disk pressure causing move failures
- High CPU on remaining brokers + recovery events → recovery traffic overloading the cluster
- RPC errors in metrics + timeout events → network or connectivity issues

### Step 4: Present findings

Rank issues by severity. For each issue:
1. State the symptom (what's wrong now)
2. Show the causal chain (what events led to this)
3. Recommend next steps

If a specific pattern emerges (decommission, partition imbalance, consumer lag), tell the user you're switching to the specialized skill and proceed with it.

## What to watch for

- `disk_usage_pct > 85%` on any broker — flag immediately
- `under_replicated > 0` — always investigate
- `leaderless > 0` — critical, investigate immediately
- Node in `DECOMMISSIONING` status — check if it's making progress
- High `rpc_latency_p99_ms` (>50ms) — indicates inter-broker communication issues
- Resource pressure events clustered on one broker — that broker is the bottleneck
