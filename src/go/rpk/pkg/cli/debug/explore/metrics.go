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
	"bufio"
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// NewMetricsCommand creates the explore-metrics command that renders
// a live-refreshing metrics view scraped from broker /public_metrics.
func NewMetricsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var refreshInterval time.Duration
	cmd := &cobra.Command{
		Use:    "explore-metrics",
		Short:  "Live cluster metrics view (used by rpk debug explore)",
		Hidden: true,
		Args:   cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			addrs := p.AdminAPI.Addresses
			if len(addrs) == 0 {
				out.Die("no admin API addresses configured")
			}

			runMetrics(cmd.Context(), addrs, refreshInterval)
		},
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	cmd.Flags().DurationVar(&refreshInterval, "refresh", 3*time.Second, "Metrics refresh interval")
	return cmd
}

var (
	mBold   = color.New(color.Bold).SprintFunc()
	mRed    = color.New(color.FgRed, color.Bold).SprintFunc()
	mYellow = color.New(color.FgYellow).SprintFunc()
	mGreen  = color.New(color.FgGreen).SprintFunc()
	mCyan   = color.New(color.FgCyan).SprintFunc()
	mDim    = color.New(color.Faint).SprintFunc()
)

type metricSample struct {
	name   string
	labels map[string]string
	value  float64
}

type brokerMetrics struct {
	addr            string
	err             error
	produceBytes    float64
	fetchBytes      float64
	topics          float64
	partitions      float64
	unavailable     float64
	underReplicated float64
	diskTotal       float64
	diskFree        float64
	rpcErrors       float64
}

func runMetrics(ctx context.Context, addrs []string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var prev []brokerMetrics

	renderMetrics(ctx, addrs, prev)
	prev = scrapeAll(ctx, addrs)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := scrapeAll(ctx, addrs)
			renderMetrics(ctx, addrs, prev)
			prev = current
		}
	}
}

func renderMetrics(ctx context.Context, addrs []string, _ []brokerMetrics) {
	current := scrapeAll(ctx, addrs)

	fmt.Print("\033[2J\033[H")

	now := time.Now().Format("15:04:05")
	fmt.Printf("%s  %s  %s\n",
		mBold("CLUSTER METRICS"),
		mDim("refreshing every 3s"),
		mDim(now))
	fmt.Println(strings.Repeat("─", 70))

	// Aggregate across brokers.
	var totalProduce, totalFetch float64
	var totalTopics, totalPartitions, totalUnavailable, totalUnderReplicated float64
	var totalRPCErrors float64
	var anyErr bool

	for _, bm := range current {
		if bm.err != nil {
			anyErr = true
			continue
		}
		totalProduce += bm.produceBytes
		totalFetch += bm.fetchBytes
		totalTopics = bm.topics // cluster-level metric, same on all brokers
		totalPartitions = bm.partitions
		totalUnavailable = bm.unavailable
		totalUnderReplicated += bm.underReplicated
		totalRPCErrors += bm.rpcErrors
	}

	// Throughput.
	fmt.Printf("\n  %s\n", mBold("Throughput"))
	fmt.Printf("  Produce: %s/s    Fetch: %s/s\n",
		mCyan(humanBytes(totalProduce)),
		mCyan(humanBytes(totalFetch)))

	// Partitions.
	fmt.Printf("\n  %s\n", mBold("Partitions"))
	fmt.Printf("  Topics: %s  Partitions: %s\n",
		mCyan(fmt.Sprintf("%.0f", totalTopics)),
		mCyan(fmt.Sprintf("%.0f", totalPartitions)))

	if totalUnavailable > 0 {
		fmt.Printf("  %s Unavailable: %s\n", mRed("⛔"), mRed(fmt.Sprintf("%.0f", totalUnavailable)))
	} else {
		fmt.Printf("  %s Unavailable: %s\n", mGreen("✓"), mGreen("0"))
	}

	if totalUnderReplicated > 0 {
		fmt.Printf("  %s Under-replicated: %s\n", mYellow("⚠"), mYellow(fmt.Sprintf("%.0f", totalUnderReplicated)))
	} else {
		fmt.Printf("  %s Under-replicated: %s\n", mGreen("✓"), mGreen("0"))
	}

	// Per-broker disk & errors.
	fmt.Printf("\n  %s\n", mBold("Brokers"))
	fmt.Printf("  %-20s %10s %10s %10s %s\n",
		mDim("Address"), mDim("Disk Used"), mDim("Disk Free"), mDim("Disk %"), mDim("RPC Err"))
	for _, bm := range current {
		if bm.err != nil {
			fmt.Printf("  %-20s %s\n", bm.addr, mRed(fmt.Sprintf("error: %v", bm.err)))
			continue
		}
		diskUsed := bm.diskTotal - bm.diskFree
		diskPct := float64(0)
		if bm.diskTotal > 0 {
			diskPct = diskUsed / bm.diskTotal * 100
		}
		pctStr := fmt.Sprintf("%.1f%%", diskPct)
		if diskPct > 90 {
			pctStr = mRed(pctStr)
		} else if diskPct > 75 {
			pctStr = mYellow(pctStr)
		}
		errStr := fmt.Sprintf("%.0f", bm.rpcErrors)
		if bm.rpcErrors > 0 {
			errStr = mRed(errStr)
		}
		fmt.Printf("  %-20s %10s %10s %10s %s\n",
			bm.addr,
			humanBytes(diskUsed),
			humanBytes(bm.diskFree),
			pctStr,
			errStr)
	}

	if anyErr {
		fmt.Printf("\n  %s Some brokers unreachable\n", mRed("⚠"))
	}

	fmt.Println()
	fmt.Println(strings.Repeat("─", 70))
	fmt.Printf("%s\n", mDim("AI assistant in bottom pane. Dashboard in top pane."))
}

func scrapeAll(ctx context.Context, addrs []string) []brokerMetrics {
	results := make([]brokerMetrics, len(addrs))
	for i, addr := range addrs {
		results[i] = scrapeBroker(ctx, addr)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].addr < results[j].addr
	})
	return results
}

func scrapeBroker(ctx context.Context, addr string) brokerMetrics {
	bm := brokerMetrics{addr: addr}

	url := fmt.Sprintf("http://%s/public_metrics", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		bm.err = err
		return bm
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		bm.err = err
		return bm
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		sample := parseLine(line)
		if sample == nil {
			continue
		}
		switch sample.name {
		case "redpanda_kafka_request_bytes_total":
			switch sample.labels["redpanda_cmd"] {
			case "produce":
				bm.produceBytes += sample.value
			case "consume", "fetch":
				bm.fetchBytes += sample.value
			}
		case "redpanda_cluster_topics":
			bm.topics = sample.value
		case "redpanda_cluster_partitions":
			bm.partitions = sample.value
		case "redpanda_cluster_unavailable_partitions":
			bm.unavailable = sample.value
		case "redpanda_kafka_under_replicated_replicas":
			bm.underReplicated += sample.value
		case "redpanda_storage_disk_total_bytes":
			bm.diskTotal = sample.value
		case "redpanda_storage_disk_free_bytes":
			bm.diskFree = sample.value
		case "redpanda_rpc_request_errors_total":
			bm.rpcErrors += sample.value
		}
	}

	return bm
}

// parseLine parses a Prometheus exposition format line like:
// parseLine parses a Prometheus exposition format line.
func parseLine(line string) *metricSample {
	if len(line) == 0 {
		return nil
	}

	var name string
	labels := make(map[string]string)
	var valueStr string

	braceStart := strings.IndexByte(line, '{')
	if braceStart >= 0 {
		name = line[:braceStart]
		braceEnd := strings.IndexByte(line, '}')
		if braceEnd < 0 {
			return nil
		}
		labelStr := line[braceStart+1 : braceEnd]
		for _, pair := range splitLabels(labelStr) {
			eqIdx := strings.IndexByte(pair, '=')
			if eqIdx < 0 {
				continue
			}
			key := pair[:eqIdx]
			val := strings.Trim(pair[eqIdx+1:], "\"")
			labels[key] = val
		}
		valueStr = strings.TrimSpace(line[braceEnd+1:])
	} else {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			return nil
		}
		name = parts[0]
		valueStr = parts[1]
	}

	val, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return nil
	}

	return &metricSample{name: name, labels: labels, value: val}
}

// splitLabels splits label pairs, respecting quoted values.
func splitLabels(s string) []string {
	var result []string
	var current strings.Builder
	inQuote := false
	for _, c := range s {
		switch {
		case c == '"':
			inQuote = !inQuote
			current.WriteRune(c)
		case c == ',' && !inQuote:
			result = append(result, current.String())
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}
	if current.Len() > 0 {
		result = append(result, current.String())
	}
	return result
}

func humanBytes(b float64) string {
	switch {
	case b >= 1e12:
		return fmt.Sprintf("%.1f TB", b/1e12)
	case b >= 1e9:
		return fmt.Sprintf("%.1f GB", b/1e9)
	case b >= 1e6:
		return fmt.Sprintf("%.1f MB", b/1e6)
	case b >= 1e3:
		return fmt.Sprintf("%.1f KB", b/1e3)
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}
