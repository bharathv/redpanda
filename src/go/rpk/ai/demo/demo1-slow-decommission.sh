#!/bin/bash
# Copyright 2026 Redpanda Data, Inc.
#
# Demo 1: Slow Decommission Mystery
# ===================================
#
# Setup: 5-node cluster, topics with data, low movement concurrency.
# Start decommission → moves trickle through → stall events fire.
#
# What Claude sees:
#   Event log: NODE_LIFECYCLE(decommission_start) → PARTITION_MOVE(start) →
#              PARTITION_MOVE(stall, learner_recovery_slow) repeating
#   Metrics:   CPU spikes on remaining brokers
#   Dashboard: UNHEALTHY if partitions get stuck
#
# Ask Claude:
#   "I started decommissioning broker 4 about 5 minutes ago and it's barely
#    making progress. Can you figure out what's going on?"
#
# Prerequisites:
#   bazel run //tools:dev_cluster -- --nodes=5 --cores=4
#   rpk profile create demo --set admin_api.addresses=127.0.0.1:9644,127.0.0.1:9645,127.0.0.1:9646,127.0.0.1:9647,127.0.0.1:9648 \
#     --set kafka_api.brokers=127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095,127.0.0.1:9096

set -euo pipefail

RPK="${RPK:-/work/redpanda/src/go/rpk/linux-amd64/rpk}"

echo "╔══════════════════════════════════════════════╗"
echo "║  Demo 1: Slow Decommission Mystery           ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# Verify cluster is up.
echo "[check] Verifying cluster..."
$RPK cluster health >/dev/null 2>&1 || {
  echo "ERROR: Cluster not reachable. Start dev_cluster first."
  exit 1
}
BROKER_COUNT=$($RPK admin brokers list 2>/dev/null | grep -c "active" || echo "0")
echo "  Found $BROKER_COUNT active brokers"
if [ "$BROKER_COUNT" -lt 4 ]; then
  echo "  WARNING: Need at least 4 brokers for this demo (have $BROKER_COUNT)"
  echo "  Restart with: bazel run //tools:dev_cluster -- --nodes=5 --cores=4"
fi
echo ""

# Pick the highest node ID to decommission.
DECOMM_NODE=$((BROKER_COUNT - 1))

# Step 1: Create topics.
echo "[1/4] Creating topics with data..."
$RPK topic create orders -p 30 -r 3 2>/dev/null || true
$RPK topic create events -p 20 -r 3 2>/dev/null || true
$RPK topic create logs -p 20 -r 3 2>/dev/null || true

for topic in orders events logs; do
  echo "  Producing to $topic..."
  for i in $(seq 1 200); do
    echo "{\"id\":$i,\"ts\":\"$(date -Iseconds)\",\"data\":\"$(head -c 500 /dev/urandom | base64 | head -c 500)\"}" |
      $RPK topic produce "$topic" -k "k-$((i % 10))" 2>/dev/null &
    # Run 10 producers in parallel.
    if ((i % 10 == 0)); then wait; fi
  done
  wait
done
echo "  Done producing."
echo ""

# Step 2: Throttle concurrency.
echo "[2/4] Injecting failure: setting partition movement concurrency to 1..."
$RPK cluster config set max_concurrent_partition_movements_per_node 1
echo ""

# Step 3: Wait for config propagation.
echo "[3/4] Waiting for config to propagate..."
sleep 3
echo ""

# Step 4: Start decommission.
echo "[4/4] Starting decommission of broker $DECOMM_NODE..."
$RPK admin brokers decommission $DECOMM_NODE
echo ""

echo "╔══════════════════════════════════════════════╗"
echo "║  Setup Complete!                              ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
echo "Wait 30-60 seconds for events to accumulate, then:"
echo ""
echo "  $RPK debug explore --ai-dir /work/redpanda/src/go/rpk/ai"
echo ""
echo "Ask Claude:"
echo "  'I started decommissioning broker $DECOMM_NODE about 5 minutes"
echo "   ago and it's barely making progress. What's going on?'"
echo ""
echo "Check events manually:"
echo "  $RPK debug event-log --since 5m"
echo ""
echo "═══ Reset commands ═══"
echo "  $RPK admin brokers recommission $DECOMM_NODE"
echo "  $RPK cluster config set max_concurrent_partition_movements_per_node 10"
echo "  $RPK topic delete orders events logs"
