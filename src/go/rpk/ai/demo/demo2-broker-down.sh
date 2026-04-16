#!/bin/bash
# Copyright 2026 Redpanda Data, Inc.
#
# Demo 2: Broker Down — Cascading Failures
# ==========================================
#
# Setup: 5-node cluster with topics, kill a broker process.
# Partitions lose replicas, leadership transfers storm, under-replicated spikes.
#
# What Claude sees:
#   Event log: LEADERSHIP_TRANSFER storm → PARTITION_MOVE(start) as balancer reacts
#   Dashboard: UNHEALTHY, nodes_down=[killed broker], under_replicated spikes
#   Charts:    Throughput dips, CPU spikes on remaining brokers
#
# Ask Claude:
#   "The cluster just went unhealthy. What happened and what should I do?"
#
# Prerequisites:
#   bazel run //tools:dev_cluster -- --nodes=5 --cores=4
#   rpk profile configured with all admin addresses

set -euo pipefail

RPK="${RPK:-/work/redpanda/src/go/rpk/linux-amd64/rpk}"

echo "╔══════════════════════════════════════════════╗"
echo "║  Demo 2: Broker Down — Cascading Failures    ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# Verify cluster is up.
echo "[check] Verifying cluster..."
$RPK cluster health >/dev/null 2>&1 || {
  echo "ERROR: Cluster not reachable."
  exit 1
}
echo ""

# Step 1: Create topics with RF=3 and produce data.
echo "[1/3] Creating topics and producing data..."
$RPK topic create payments -p 30 -r 3 2>/dev/null || true
$RPK topic create user-events -p 20 -r 3 2>/dev/null || true

for topic in payments user-events; do
  echo "  Producing to $topic..."
  for i in $(seq 1 300); do
    echo "{\"id\":$i,\"amount\":$((RANDOM % 10000))}" |
      $RPK topic produce "$topic" -k "k-$((i % 15))" 2>/dev/null &
    if ((i % 15 == 0)); then wait; fi
  done
  wait
done
echo "  Done producing."
echo ""

# Step 2: Find the broker to kill (pick node 2 — middle of the cluster).
KILL_NODE=2
echo "[2/3] Identifying broker to kill: node $KILL_NODE"

# Find the PID of the redpanda process for this node.
KILL_PID=$(ps aux | grep "redpanda.*node${KILL_NODE}/config" | grep -v grep | awk '{print $2}')

if [ -z "$KILL_PID" ]; then
  echo "  WARNING: Could not find PID for node $KILL_NODE"
  echo "  Listing running redpanda processes:"
  ps aux | grep redpanda | grep -v grep | grep -v bazel | grep -v clangd
  echo ""
  echo "  Find the right PID and run: kill -9 <PID>"
  echo "  Then launch the explorer."
  exit 1
fi

echo "  Found PID $KILL_PID for node $KILL_NODE"
echo ""

# Step 3: Show what we're about to do.
echo "[3/3] Ready to kill broker $KILL_NODE (PID $KILL_PID)"
echo ""
echo "  This will:"
echo "  - Kill the redpanda process for node $KILL_NODE"
echo "  - Cause leadership transfers for all partitions led by that node"
echo "  - Cause under-replicated partitions (RF=3, now only 2 copies)"
echo "  - Trigger the partition balancer to move partitions off"
echo ""
read -p "  Press Enter to kill broker $KILL_NODE, or Ctrl-C to abort... "

echo ""
echo "  Killing broker $KILL_NODE (PID $KILL_PID)..."
kill -9 "$KILL_PID" 2>/dev/null || true

echo "  Broker killed!"
echo ""

echo "╔══════════════════════════════════════════════╗"
echo "║  Setup Complete!                              ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
echo "Wait 10-20 seconds for the cluster to detect the failure, then:"
echo ""
echo "  $RPK debug explore --ai-dir /work/redpanda/src/go/rpk/ai"
echo ""
echo "Ask Claude:"
echo "  'The cluster just went unhealthy. What happened and what should I do?'"
echo ""
echo "Or more specific:"
echo "  'I'm seeing under-replicated partitions. Can you diagnose?'"
echo ""
echo "Check events:"
echo "  $RPK debug event-log --since 5m"
echo ""
echo "═══ Recovery ═══"
echo "  Restart the dev_cluster to bring the broker back:"
echo "  bazel run //tools:dev_cluster -- --nodes=5 --cores=4"
echo ""
echo "  Or clean up topics:"
echo "  $RPK topic delete payments user-events"
