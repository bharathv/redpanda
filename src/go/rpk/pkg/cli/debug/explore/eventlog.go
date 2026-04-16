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
	"io"
	"net/http"
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

// NewEventLogCommand creates the event-log command that queries diagnostic
// events from each broker.
func NewEventLogCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		since     string
		severity  string
		subsystem string
		broker    string
	)
	cmd := &cobra.Command{
		Use:   "event-log",
		Short: "Query diagnostic events from broker event logs",
		Long: `Query diagnostic events from Redpanda brokers.

Each broker maintains a per-shard ring buffer of diagnostic events emitted by
subsystems (raft, partition_manager, storage, etc.). This command queries the
event log and displays events in reverse chronological order.

By default, queries all configured brokers. Use --broker to target a specific
broker.
`,
		Example: `
Get recent warning/error events from all brokers:
  rpk debug event-log --since 15m --severity warn

Get events from a specific broker:
  rpk debug event-log --broker 127.0.0.1:9644 --since 1h

Filter by subsystem:
  rpk debug event-log --since 30m --subsystem partition_manager
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]eventLogEntry{}); ok {
				out.Exit(h)
			}
			prof, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(prof)

			cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			addrs := prof.AdminAPI.Addresses
			if broker != "" {
				addrs = []string{broker}
			}

			sinceSeconds, err := parseDuration(since)
			out.MaybeDie(err, "invalid --since value: %v", err)

			events := queryEventLogs(cmd.Context(), cl, addrs, sinceSeconds, severity, subsystem)

			if isText, _, formatted, err := f.Format(events); !isText {
				out.MaybeDie(err, "unable to format: %v", err)
				fmt.Println(formatted)
				return
			}
			printEventLog(events)
		},
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	p.InstallFormatFlag(cmd)
	cmd.Flags().StringVar(&since, "since", "15m", "Only show events newer than this duration (e.g. 15m, 1h, 30s)")
	cmd.Flags().StringVar(&severity, "severity", "", "Minimum severity filter: info, warn, error")
	cmd.Flags().StringVar(&subsystem, "subsystem", "", "Filter by subsystem (comma-separated): raft, partition_manager, storage, node_lifecycle, resource_monitor, config, rpc")
	cmd.Flags().StringVar(&broker, "broker", "", "Query a specific broker address instead of all")
	return cmd
}

type eventLogEntry struct {
	Broker    string `json:"broker" yaml:"broker"`
	Timestamp string `json:"timestamp" yaml:"timestamp"`
	Severity  string `json:"severity" yaml:"severity"`
	Subsystem string `json:"subsystem" yaml:"subsystem"`
	Partition string `json:"partition,omitempty" yaml:"partition,omitempty"`
	Event     string `json:"event" yaml:"event"`
	Details   string `json:"details,omitempty" yaml:"details,omitempty"`
}

// eventLogResponse matches the ConnectRPC JSON returned by DiagnosticsService.
// ConnectRPC uses camelCase field names.
type eventLogResponse struct {
	Events []struct {
		TimestampNs json.Number `json:"timestampNs"`
		ShardID     int         `json:"shardId"`
		Severity    string      `json:"severity"`
		Subsystem   string      `json:"subsystem"`
		Partition   *struct {
			Namespace string `json:"namespace"`
			Topic     string `json:"topic"`
			ID        int    `json:"id"`
		} `json:"partition,omitempty"`

		// Event payloads — only one will be set (ConnectRPC camelCase).
		LeadershipTransfer *json.RawMessage `json:"leadershipTransfer,omitempty"`
		PartitionMove      *json.RawMessage `json:"partitionMove,omitempty"`
		ResourcePressure   *json.RawMessage `json:"resourcePressure,omitempty"`
		NodeLifecycle      *json.RawMessage `json:"nodeLifecycle,omitempty"`
		ReplicaSetChange   *json.RawMessage `json:"replicaSetChange,omitempty"`
		ConfigChange       *json.RawMessage `json:"configChange,omitempty"`
		Error              *json.RawMessage `json:"error,omitempty"`
	} `json:"events"`
	BufferCapacity    int `json:"bufferCapacity"`
	TotalEventsStored int `json:"totalEventsStored"`
}

func queryEventLogs(ctx context.Context, cl *rpadmin.AdminAPI, _ []string, sinceSeconds int64, severity, subsystem string) []eventLogEntry {
	// Get the broker list from the seed node.
	brokers, err := cl.Brokers(ctx)
	if err != nil {
		return []eventLogEntry{{Broker: "?", Event: fmt.Sprintf("unable to list brokers: %v", err)}}
	}

	var allEntries []eventLogEntry
	for _, b := range brokers {
		// Get a client scoped to this specific broker. ForBroker discovers
		// the admin address by querying /v1/node_config on each seed.
		brokerCl, err := cl.ForBroker(ctx, b.NodeID)
		if err != nil {
			allEntries = append(allEntries, eventLogEntry{
				Broker: fmt.Sprintf("%d", b.NodeID),
				Event:  fmt.Sprintf("error: %v", err),
			})
			continue
		}
		entries := queryBrokerEventLogRPC(ctx, brokerCl, sinceSeconds, severity, subsystem)
		label := fmt.Sprintf("%d", b.NodeID)
		for i := range entries {
			entries[i].Broker = label
		}
		allEntries = append(allEntries, entries...)
	}
	return allEntries
}

// queryBrokerEventLogRPC uses the rpadmin client (which knows the broker's
// admin URL) to send the ConnectRPC request.
func queryBrokerEventLogRPC(ctx context.Context, cl *rpadmin.AdminAPI, sinceSeconds int64, severity, subsystem string) []eventLogEntry {
	reqBody := map[string]any{}
	if sinceSeconds > 0 {
		reqBody["sinceSeconds"] = fmt.Sprintf("%d", sinceSeconds)
	}
	if severity != "" {
		reqBody["minSeverity"] = mapSeverity(severity)
	}
	if subsystem != "" {
		reqBody["subsystemFilter"] = mapSubsystem(subsystem)
	}

	bodyBytes, _ := json.Marshal(reqBody)
	path := "/redpanda.core.admin.v2.DiagnosticsService/GetEventLog"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, path, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return []eventLogEntry{{Event: fmt.Sprintf("error: %v", err)}}
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := cl.Do(httpReq)
	if err != nil {
		return []eventLogEntry{{Event: fmt.Sprintf("error: %v", err)}}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return []eventLogEntry{{Event: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))}}
	}

	var elResp eventLogResponse
	if err := json.NewDecoder(resp.Body).Decode(&elResp); err != nil {
		return []eventLogEntry{{Event: fmt.Sprintf("decode error: %v", err)}}
	}

	var entries []eventLogEntry
	for _, ev := range elResp.Events {
		tsNs, _ := ev.TimestampNs.Int64()
		ts := time.Unix(0, tsNs).Format("15:04:05")
		partition := ""
		if ev.Partition != nil {
			partition = fmt.Sprintf("%s/%s/%d", ev.Partition.Namespace, ev.Partition.Topic, ev.Partition.ID)
		}
		eventType, details := classifyEvent(ev)
		entries = append(entries, eventLogEntry{
			Timestamp: ts,
			Severity:  ev.Severity,
			Subsystem: ev.Subsystem,
			Partition: partition,
			Event:     eventType,
			Details:   details,
		})
	}
	return entries
}

func classifyEvent(ev struct {
	TimestampNs json.Number `json:"timestampNs"`
	ShardID     int         `json:"shardId"`
	Severity    string      `json:"severity"`
	Subsystem   string      `json:"subsystem"`
	Partition   *struct {
		Namespace string `json:"namespace"`
		Topic     string `json:"topic"`
		ID        int    `json:"id"`
	} `json:"partition,omitempty"`
	LeadershipTransfer *json.RawMessage `json:"leadershipTransfer,omitempty"`
	PartitionMove      *json.RawMessage `json:"partitionMove,omitempty"`
	ResourcePressure   *json.RawMessage `json:"resourcePressure,omitempty"`
	NodeLifecycle      *json.RawMessage `json:"nodeLifecycle,omitempty"`
	ReplicaSetChange   *json.RawMessage `json:"replicaSetChange,omitempty"`
	ConfigChange       *json.RawMessage `json:"configChange,omitempty"`
	Error              *json.RawMessage `json:"error,omitempty"`
},
) (eventType string, details string) {
	switch {
	case ev.LeadershipTransfer != nil:
		return "LEADERSHIP_TRANSFER", string(*ev.LeadershipTransfer)
	case ev.PartitionMove != nil:
		return "PARTITION_MOVE", string(*ev.PartitionMove)
	case ev.ResourcePressure != nil:
		return "RESOURCE_PRESSURE", string(*ev.ResourcePressure)
	case ev.NodeLifecycle != nil:
		return "NODE_LIFECYCLE", string(*ev.NodeLifecycle)
	case ev.ReplicaSetChange != nil:
		return "REPLICA_SET_CHANGE", string(*ev.ReplicaSetChange)
	case ev.ConfigChange != nil:
		return "CONFIG_CHANGE", string(*ev.ConfigChange)
	case ev.Error != nil:
		return "ERROR", string(*ev.Error)
	default:
		return "UNKNOWN", ""
	}
}

var (
	elRed    = color.New(color.FgRed, color.Bold).SprintFunc()
	elYellow = color.New(color.FgYellow).SprintFunc()
	elDim    = color.New(color.Faint).SprintFunc()
	elCyan   = color.New(color.FgCyan).SprintFunc()
)

func printEventLog(entries []eventLogEntry) {
	if len(entries) == 0 {
		fmt.Println("No diagnostic events found.")
		return
	}

	tw := out.NewTable("TIME", "BROKER", "SEV", "SUBSYSTEM", "EVENT", "PARTITION", "DETAILS")
	defer tw.Flush()

	for _, e := range entries {
		sev := e.Severity
		switch strings.ToLower(sev) {
		case "error", "severity_error":
			sev = elRed("ERR")
		case "warn", "severity_warn":
			sev = elYellow("WRN")
		default:
			sev = elDim("INF")
		}

		details := e.Details
		if len(details) > 60 {
			details = details[:57] + "..."
		}

		tw.Print(
			elCyan(e.Timestamp),
			e.Broker,
			sev,
			e.Subsystem,
			e.Event,
			e.Partition,
			details,
		)
	}
}

func parseDuration(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return int64(d.Seconds()), nil
}

func mapSeverity(s string) string {
	switch strings.ToLower(s) {
	case "info":
		return "SEVERITY_INFO"
	case "warn", "warning":
		return "SEVERITY_WARN"
	case "error", "err":
		return "SEVERITY_ERROR"
	default:
		return s
	}
}

func mapSubsystem(s string) string {
	parts := strings.Split(s, ",")
	var mapped []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		mapped = append(mapped, "SUBSYSTEM_"+strings.ToUpper(p))
	}
	return strings.Join(mapped, ",")
}
