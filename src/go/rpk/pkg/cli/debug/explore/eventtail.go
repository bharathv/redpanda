// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package explore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewEventTailCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var refreshInterval time.Duration
	cmd := &cobra.Command{
		Use:    "explore-event-tail",
		Short:  "Live tailing view of diagnostic events (used by rpk debug explore)",
		Hidden: true,
		Args:   cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			runEventTail(cmd.Context(), cl, refreshInterval)
		},
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	cmd.Flags().DurationVar(&refreshInterval, "refresh", 3*time.Second, "Poll interval")
	return cmd
}

var (
	tBold   = color.New(color.Bold).SprintFunc()
	tRed    = color.New(color.FgRed, color.Bold).SprintFunc()
	tYellow = color.New(color.FgYellow).SprintFunc()
	tGreen  = color.New(color.FgGreen).SprintFunc()
	tCyan   = color.New(color.FgCyan).SprintFunc()
	tDim    = color.New(color.Faint).SprintFunc()
)

func runEventTail(ctx context.Context, cl *rpadmin.AdminAPI, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastCount int
	seenKeys := make(map[string]bool)

	// Initial render.
	renderEventTail(ctx, cl, seenKeys, &lastCount)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renderEventTail(ctx, cl, seenKeys, &lastCount)
		}
	}
}

func renderEventTail(ctx context.Context, cl *rpadmin.AdminAPI, _ map[string]bool, _ *int) {
	brokers, err := cl.Brokers(ctx)
	if err != nil {
		return
	}

	var allEvents []eventTailEntry
	for _, b := range brokers {
		brokerCl, err := cl.ForBroker(ctx, b.NodeID)
		if err != nil {
			continue
		}
		events := fetchRecentEvents(ctx, brokerCl, b.NodeID)
		allEvents = append(allEvents, events...)
	}

	// Always redraw with latest events.
	fmt.Print("\033[2J\033[H")

	now := time.Now().Format("15:04:05")
	fmt.Printf("%s  %s  %s\n",
		tBold("EVENT LOG"),
		tDim("tailing every 3s"),
		tDim(now))
	fmt.Println(strings.Repeat("─", 70))

	if len(allEvents) == 0 {
		fmt.Printf("  %s\n", tDim("No events yet. Waiting for cluster activity..."))
		return
	}

	// Show last few events from each broker so no broker's events are hidden.
	perBroker := make(map[int][]eventTailEntry)
	for _, ev := range allEvents {
		perBroker[ev.nodeID] = append(perBroker[ev.nodeID], ev)
	}
	var display []eventTailEntry
	for _, events := range perBroker {
		start := 0
		if len(events) > 3 {
			start = len(events) - 3
		}
		display = append(display, events[start:]...)
	}
	// Sort by timestamp for display.
	sort.Slice(display, func(i, j int) bool {
		return display[i].timestamp < display[j].timestamp
	})
	for _, ev := range display {
		sev := formatSeverity(ev.severity)
		partition := ev.partition
		if partition == "" {
			partition = tDim("—")
		}
		details := ev.details
		if len(details) > 40 {
			details = details[:37] + "..."
		}
		fmt.Printf("  %s %s %s %-20s %-25s %s\n",
			tCyan(ev.timestamp),
			fmt.Sprintf("n%d", ev.nodeID),
			sev,
			ev.event,
			partition,
			tDim(details))
	}
}

func formatSeverity(s string) string {
	switch strings.ToLower(s) {
	case "severity_error":
		return tRed("ERR")
	case "severity_warn":
		return tYellow("WRN")
	default:
		return tGreen("INF")
	}
}

type eventTailEntry struct {
	nodeID    int
	timestamp string
	severity  string
	event     string
	partition string
	details   string
}

func fetchRecentEvents(ctx context.Context, cl *rpadmin.AdminAPI, nodeID int) []eventTailEntry {
	reqBody := map[string]any{
		"sinceSeconds": "300",
	}
	bodyBytes, _ := json.Marshal(reqBody)
	path := "/redpanda.core.admin.v2.DiagnosticsService/GetEventLog"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, path, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return nil
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := cl.Do(httpReq)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var elResp eventLogResponse
	if err := json.NewDecoder(resp.Body).Decode(&elResp); err != nil {
		return nil
	}

	var entries []eventTailEntry
	for _, ev := range elResp.Events {
		tsNs, _ := ev.TimestampNs.Int64()
		ts := time.Unix(0, tsNs).Format("15:04:05")
		partition := ""
		if ev.Partition != nil {
			partition = fmt.Sprintf("%s/%s/%d", ev.Partition.Namespace, ev.Partition.Topic, ev.Partition.ID)
		}
		eventType, details := classifyEvent(ev)
		entries = append(entries, eventTailEntry{
			nodeID:    nodeID,
			timestamp: ts,
			severity:  ev.Severity,
			event:     eventType,
			partition: partition,
			details:   details,
		})
	}
	return entries
}
