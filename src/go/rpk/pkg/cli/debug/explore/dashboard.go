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
	"fmt"
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

// NewDashboardCommand creates the explore-dashboard command that renders
// a live-refreshing cluster health view.
func NewDashboardCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var refreshInterval time.Duration
	cmd := &cobra.Command{
		Use:    "explore-dashboard",
		Short:  "Live cluster health dashboard (used by rpk debug explore)",
		Hidden: true,
		Args:   cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			runDashboard(cmd.Context(), cl, refreshInterval)
		},
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	cmd.Flags().DurationVar(&refreshInterval, "refresh", 5*time.Second, "Dashboard refresh interval")
	return cmd
}

var (
	bold    = color.New(color.Bold).SprintFunc()
	red     = color.New(color.FgRed, color.Bold).SprintFunc()
	yellow  = color.New(color.FgYellow).SprintFunc()
	green   = color.New(color.FgGreen).SprintFunc()
	cyan    = color.New(color.FgCyan).SprintFunc()
	dim     = color.New(color.Faint).SprintFunc()
	redBg   = color.New(color.BgRed, color.FgWhite, color.Bold).SprintFunc()
	greenBg = color.New(color.BgGreen, color.FgBlack).SprintFunc()
)

func runDashboard(ctx context.Context, cl *rpadmin.AdminAPI, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial render.
	render(ctx, cl)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			render(ctx, cl)
		}
	}
}

func render(ctx context.Context, cl *rpadmin.AdminAPI) {
	health, healthErr := cl.GetHealthOverview(ctx)

	// Clear screen and move cursor to top.
	fmt.Print("\033[2J\033[H")

	now := time.Now().Format("15:04:05")
	fmt.Printf("%s  %s  %s\n",
		bold("REDPANDA CLUSTER HEALTH"),
		dim("refreshing every 5s"),
		dim(now))
	fmt.Println(strings.Repeat("─", 60))

	if healthErr != nil {
		fmt.Printf("%s %v\n", red("ERROR:"), healthErr)
		return
	}

	// Health status banner.
	if health.IsHealthy {
		fmt.Printf("  Status: %s\n", greenBg(" HEALTHY "))
	} else {
		fmt.Printf("  Status: %s\n", redBg(" UNHEALTHY "))
		for _, reason := range health.UnhealthyReasons {
			fmt.Printf("    %s %s\n", red("▸"), reason)
		}
	}
	fmt.Println()

	// Node overview.
	fmt.Printf("  %s  Controller: %s\n",
		bold("Nodes"),
		cyan(fmt.Sprintf("broker-%d", health.ControllerID)))
	fmt.Printf("  All:  %v\n", formatNodeList(health.AllNodes))
	if len(health.NodesDown) > 0 {
		fmt.Printf("  Down: %s\n", red(formatNodeList(health.NodesDown)))
	}
	if len(health.NodesInRecoveryMode) > 0 {
		fmt.Printf("  Recovery: %s\n", yellow(formatNodeList(health.NodesInRecoveryMode)))
	}
	fmt.Println()

	// Critical signals.
	fmt.Printf("  %s\n", bold("Signals"))

	// Leaderless partitions.
	var leaderlessCount int
	if health.LeaderlessCount != nil {
		leaderlessCount = *health.LeaderlessCount
	} else {
		leaderlessCount = len(health.LeaderlessPartitions)
	}
	if leaderlessCount > 0 {
		fmt.Printf("  %s Leaderless partitions: %s\n",
			red("⛔"),
			red(fmt.Sprintf("%d", leaderlessCount)))
		for i, p := range health.LeaderlessPartitions {
			if i >= 5 {
				fmt.Printf("       %s\n", dim(fmt.Sprintf("...and %d more", leaderlessCount-5)))
				break
			}
			fmt.Printf("       %s\n", p)
		}
	} else {
		fmt.Printf("  %s Leaderless partitions: %s\n", green("✓"), green("0"))
	}

	// Under-replicated partitions.
	var urCount int
	if health.UnderReplicatedCount != nil {
		urCount = *health.UnderReplicatedCount
	} else {
		urCount = len(health.UnderReplicatedPartitions)
	}
	if urCount > 0 {
		fmt.Printf("  %s Under-replicated:      %s\n",
			yellow("⚠"),
			yellow(fmt.Sprintf("%d", urCount)))
		for i, p := range health.UnderReplicatedPartitions {
			if i >= 5 {
				fmt.Printf("       %s\n", dim(fmt.Sprintf("...and %d more", urCount-5)))
				break
			}
			fmt.Printf("       %s\n", p)
		}
	} else {
		fmt.Printf("  %s Under-replicated:      %s\n", green("✓"), green("0"))
	}

	// High disk usage nodes.
	if len(health.HighDiskUsageNodes) > 0 {
		fmt.Printf("  %s High disk usage:       %s\n",
			red("⛔"),
			red(fmt.Sprintf("brokers %v", health.HighDiskUsageNodes)))
	} else {
		fmt.Printf("  %s Disk usage:            %s\n", green("✓"), green("normal"))
	}

	fmt.Println()
	fmt.Println(strings.Repeat("─", 60))
	fmt.Printf("%s\n", dim("AI assistant running in pane below. Press Ctrl-C to exit."))
}

func formatNodeList(nodes []int) string {
	if len(nodes) == 0 {
		return dim("none")
	}
	parts := make([]string, len(nodes))
	for i, n := range nodes {
		parts[i] = fmt.Sprintf("broker-%d", n)
	}
	return strings.Join(parts, ", ")
}
