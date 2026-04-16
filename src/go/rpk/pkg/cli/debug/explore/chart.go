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
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewChartCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var refreshInterval time.Duration
	cmd := &cobra.Command{
		Use:    "explore-chart",
		Short:  "Live metric charts (used by rpk debug explore)",
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

			runChart(cmd.Context(), addrs, refreshInterval)
		},
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	cmd.Flags().DurationVar(&refreshInterval, "refresh", 3*time.Second, "Scrape interval")
	return cmd
}

var (
	cBold   = color.New(color.Bold).SprintFunc()
	cRed    = color.New(color.FgRed, color.Bold).SprintFunc()
	cYellow = color.New(color.FgYellow).SprintFunc()
	cGreen  = color.New(color.FgGreen).SprintFunc()
	cCyan   = color.New(color.FgCyan).SprintFunc()
	cDim    = color.New(color.Faint).SprintFunc()
	cWhite  = color.New(color.FgWhite, color.Bold).SprintFunc()
	cBlue   = color.New(color.FgBlue).SprintFunc()
)

const maxDataPoints = 60

type timeSeries struct {
	values []float64
	times  []time.Time
}

func (ts *timeSeries) add(t time.Time, v float64) {
	ts.values = append(ts.values, v)
	ts.times = append(ts.times, t)
	if len(ts.values) > maxDataPoints {
		ts.values = ts.values[1:]
		ts.times = ts.times[1:]
	}
}

func (ts *timeSeries) rates() []float64 {
	if len(ts.values) < 2 {
		return nil
	}
	r := make([]float64, len(ts.values)-1)
	for i := 1; i < len(ts.values); i++ {
		dt := ts.times[i].Sub(ts.times[i-1]).Seconds()
		if dt > 0 {
			r[i-1] = math.Max(0, (ts.values[i]-ts.values[i-1])/dt)
		}
	}
	return r
}

// perBrokerSnapshot holds latest metric values per broker.
type perBrokerSnapshot struct {
	addr       string
	nodeID     int
	diskPct    float64
	memAlloc   float64
	memFree    float64
	produce    float64
	fetch      float64
	rpcErr     float64
	cpuRuntime float64 // scheduler_runtime_seconds_total (sum across groups)
	ioReads    float64
	ioWrites   float64
}

type chartState struct {
	series  map[string]*timeSeries
	brokers []perBrokerSnapshot
	addrs   []string
}

func runChart(ctx context.Context, addrs []string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	selectedView := 0
	views := []string{"Overview", "Throughput", "Disk", "CPU & Memory"}

	state := &chartState{
		series: make(map[string]*timeSeries),
		addrs:  addrs,
	}

	keyCh := make(chan rune, 10)
	go readKeys(keyCh)

	scrapeAll2(ctx, state)
	renderChartView(state, views, selectedView)

	for {
		select {
		case <-ctx.Done():
			return
		case key := <-keyCh:
			switch key {
			case 'j', 'J', ']':
				selectedView = (selectedView + 1) % len(views)
			case 'k', 'K', '[':
				selectedView = (selectedView - 1 + len(views)) % len(views)
			case '1', '2', '3', '4':
				idx := int(key - '1')
				if idx < len(views) {
					selectedView = idx
				}
			}
			renderChartView(state, views, selectedView)
		case <-ticker.C:
			scrapeAll2(ctx, state)
			renderChartView(state, views, selectedView)
		}
	}
}

func readKeys(ch chan<- rune) {
	reader := bufio.NewReader(os.Stdin)
	for {
		r, _, err := reader.ReadRune()
		if err != nil {
			return
		}
		ch <- r
	}
}

func scrapeAll2(ctx context.Context, state *chartState) {
	now := time.Now()
	var brokers []perBrokerSnapshot

	var totalProduce, totalFetch, totalRPCErr, totalCPU float64
	var totalMemAlloc, totalMemFree, totalIOReads, totalIOWrites float64

	for i, addr := range state.addrs {
		bm := scrapeBrokerFull(ctx, addr)
		snap := perBrokerSnapshot{
			addr:       addr,
			nodeID:     i,
			produce:    bm.produceBytes,
			fetch:      bm.fetchBytes,
			rpcErr:     bm.rpcErrors,
			cpuRuntime: bm.cpuRuntime,
			memAlloc:   bm.memAlloc,
			memFree:    bm.memFree,
			ioReads:    bm.ioReads,
			ioWrites:   bm.ioWrites,
		}
		if bm.diskTotal > 0 {
			snap.diskPct = (bm.diskTotal - bm.diskFree) / bm.diskTotal * 100
		}
		totalProduce += bm.produceBytes
		totalFetch += bm.fetchBytes
		totalRPCErr += bm.rpcErrors
		totalCPU += bm.cpuRuntime
		totalMemAlloc += bm.memAlloc
		totalMemFree += bm.memFree
		totalIOReads += bm.ioReads
		totalIOWrites += bm.ioWrites
		brokers = append(brokers, snap)
	}
	state.brokers = brokers

	store := func(key string, val float64) {
		ts, ok := state.series[key]
		if !ok {
			ts = &timeSeries{}
			state.series[key] = ts
		}
		ts.add(now, val)
	}

	store("produce_total", totalProduce)
	store("fetch_total", totalFetch)
	store("rpc_errors_total", totalRPCErr)
	store("cpu_runtime_total", totalCPU)
	store("mem_alloc_total", totalMemAlloc)
	store("mem_free_total", totalMemFree)
	store("io_reads_total", totalIOReads)
	store("io_writes_total", totalIOWrites)
	for _, b := range brokers {
		store(fmt.Sprintf("disk_pct_%d", b.nodeID), b.diskPct)
		store(fmt.Sprintf("cpu_runtime_%d", b.nodeID), b.cpuRuntime)
		store(fmt.Sprintf("mem_alloc_%d", b.nodeID), b.memAlloc)
	}
}

type fullBrokerMetrics struct {
	produceBytes float64
	fetchBytes   float64
	diskTotal    float64
	diskFree     float64
	rpcErrors    float64
	cpuRuntime   float64 // sum of redpanda_scheduler_runtime_seconds_total
	memAlloc     float64 // memory_allocated_memory
	memFree      float64 // memory_free_memory
	ioReads      float64 // io_queue_total_read_ops
	ioWrites     float64 // io_queue_total_write_ops
}

func scrapeBrokerFull(ctx context.Context, addr string) fullBrokerMetrics {
	var bm fullBrokerMetrics
	url := fmt.Sprintf("http://%s/public_metrics", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return bm
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
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
		case "redpanda_storage_disk_total_bytes":
			bm.diskTotal = sample.value
		case "redpanda_storage_disk_free_bytes":
			bm.diskFree = sample.value
		case "redpanda_rpc_request_errors_total":
			bm.rpcErrors += sample.value
		case "redpanda_scheduler_runtime_seconds_total":
			bm.cpuRuntime += sample.value
		case "vectorized_memory_allocated_memory", "memory_allocated_memory":
			bm.memAlloc += sample.value
		case "vectorized_memory_free_memory", "memory_free_memory":
			bm.memFree += sample.value
		case "vectorized_io_queue_total_read_ops", "io_queue_total_read_ops":
			bm.ioReads += sample.value
		case "vectorized_io_queue_total_write_ops", "io_queue_total_write_ops":
			bm.ioWrites += sample.value
		}
	}
	return bm
}

func renderChartView(state *chartState, views []string, selected int) {
	fmt.Print("\033[2J\033[H")

	now := time.Now().Format("15:04:05")
	fmt.Printf("%s  %s\n", cBold("CHARTS"), cDim(now))

	var tabs []string
	for i, v := range views {
		if i == selected {
			tabs = append(tabs, cWhite(fmt.Sprintf("[%d:%s]", i+1, v)))
		} else {
			tabs = append(tabs, cDim(fmt.Sprintf(" %d:%s ", i+1, v)))
		}
	}
	fmt.Println(strings.Join(tabs, " "))
	fmt.Println(strings.Repeat("─", 50))

	switch selected {
	case 0:
		renderOverview(state)
	case 1:
		renderThroughput(state)
	case 2:
		renderDisk(state)
	case 3:
		renderResources(state)
	}

	fmt.Println(strings.Repeat("─", 50))
	fmt.Printf("%s\n", cDim("Ctrl-B ↑ to focus, 1-4/j/k to switch"))
}

func renderOverview(state *chartState) {
	// Per-broker bar chart showing disk usage.
	fmt.Printf("\n  %s\n\n", cBold("Disk Usage Per Broker"))
	for _, b := range state.brokers {
		bar := horizontalBar(b.diskPct, 30)
		pctStr := fmt.Sprintf("%.1f%%", b.diskPct)
		if b.diskPct > 90 {
			pctStr = cRed(pctStr)
		} else if b.diskPct > 75 {
			pctStr = cYellow(pctStr)
		}
		fmt.Printf("  n%-2d %s %s\n", b.nodeID, bar, pctStr)
	}

	// Throughput sparklines.
	fmt.Printf("\n  %s\n", cBold("Throughput"))
	if ts, ok := state.series["produce_total"]; ok {
		rates := ts.rates()
		if len(rates) > 0 {
			last := rates[len(rates)-1]
			fmt.Printf("  Produce  %s  %s\n", sparkline(rates, 30), cCyan(humanVal(last)+"/s"))
		}
	}
	if ts, ok := state.series["fetch_total"]; ok {
		rates := ts.rates()
		if len(rates) > 0 {
			last := rates[len(rates)-1]
			fmt.Printf("  Fetch    %s  %s\n", sparkline(rates, 30), cCyan(humanVal(last)+"/s"))
		}
	}

	// CPU.
	if ts, ok := state.series["cpu_runtime_total"]; ok {
		rates := ts.rates()
		if len(rates) > 0 {
			last := rates[len(rates)-1]
			fmt.Printf("  CPU      %s  %s\n", sparkline(rates, 30), cCyan(fmt.Sprintf("%.1f cores", last)))
		}
	}

	// Memory.
	fmt.Printf("\n  %s\n", cBold("Memory Per Broker"))
	for _, b := range state.brokers {
		total := b.memAlloc + b.memFree
		pct := float64(0)
		if total > 0 {
			pct = b.memAlloc / total * 100
		}
		bar := horizontalBar(pct, 20)
		fmt.Printf("  n%-2d %s %.0f%%\n", b.nodeID, bar, pct)
	}

	// RPC errors.
	fmt.Printf("\n  %s\n", cBold("Errors"))
	if ts, ok := state.series["rpc_errors_total"]; ok {
		rates := ts.rates()
		if len(rates) > 0 {
			last := rates[len(rates)-1]
			errStr := humanVal(last) + "/s"
			if last > 0 {
				errStr = cRed(errStr)
			} else {
				errStr = cGreen(errStr)
			}
			fmt.Printf("  RPC Err  %s  %s\n", sparkline(rates, 30), errStr)
		}
	}
}

func renderThroughput(state *chartState) {
	fmt.Printf("\n  %s\n\n", cBold("Produce Throughput"))
	if ts, ok := state.series["produce_total"]; ok {
		rates := ts.rates()
		if len(rates) > 2 {
			last := rates[len(rates)-1]
			fmt.Printf("  Current: %s\n\n", cCyan(humanVal(last)+"/s"))
			printAreaChart(rates, 8, 45, cGreen)
		} else {
			fmt.Printf("  %s\n", cDim("accumulating data..."))
		}
	}

	fmt.Printf("\n  %s\n\n", cBold("Fetch Throughput"))
	if ts, ok := state.series["fetch_total"]; ok {
		rates := ts.rates()
		if len(rates) > 2 {
			last := rates[len(rates)-1]
			fmt.Printf("  Current: %s\n\n", cCyan(humanVal(last)+"/s"))
			printAreaChart(rates, 8, 45, cBlue)
		} else {
			fmt.Printf("  %s\n", cDim("accumulating data..."))
		}
	}
}

func renderDisk(state *chartState) {
	fmt.Printf("\n  %s\n\n", cBold("Disk Usage Per Broker (bar + history)"))

	for _, b := range state.brokers {
		pctStr := fmt.Sprintf("%.1f%%", b.diskPct)
		if b.diskPct > 90 {
			pctStr = cRed(pctStr)
		} else if b.diskPct > 75 {
			pctStr = cYellow(pctStr)
		}
		bar := horizontalBar(b.diskPct, 35)
		fmt.Printf("  n%-2d %s %s\n", b.nodeID, bar, pctStr)

		// History sparkline.
		key := fmt.Sprintf("disk_pct_%d", b.nodeID)
		if ts, ok := state.series[key]; ok && len(ts.values) > 1 {
			fmt.Printf("      %s\n", sparkline(ts.values, 35))
		}
		fmt.Println()
	}
}

func renderResources(state *chartState) {
	// CPU: rate of scheduler_runtime_seconds_total = CPU seconds/s per core.
	// On a 4-core broker, max is 4.0 (400%).
	fmt.Printf("\n  %s\n\n", cBold("CPU Usage (scheduler runtime rate)"))
	if ts, ok := state.series["cpu_runtime_total"]; ok {
		rates := ts.rates()
		if len(rates) > 2 {
			last := rates[len(rates)-1]
			// Total cores across all brokers.
			numBrokers := len(state.brokers)
			if numBrokers == 0 {
				numBrokers = 1
			}
			fmt.Printf("  Current: %s  %s\n\n",
				cCyan(fmt.Sprintf("%.2f cores busy", last)),
				cDim(fmt.Sprintf("(%d brokers)", numBrokers)))
			printAreaChart(rates, 6, 45, cCyan)
		} else {
			fmt.Printf("  %s\n", cDim("accumulating data..."))
		}
	}

	// Per-broker CPU bar.
	fmt.Printf("\n  %s\n\n", cBold("CPU Per Broker"))
	for _, b := range state.brokers {
		key := fmt.Sprintf("cpu_runtime_%d", b.nodeID)
		if ts, ok := state.series[key]; ok {
			rates := ts.rates()
			if len(rates) > 0 {
				last := rates[len(rates)-1]
				// Approximate: 4 cores = 4.0 max, show as percentage of 4.
				pct := last / 4.0 * 100
				if pct > 100 {
					pct = 100
				}
				bar := horizontalBar(pct, 30)
				fmt.Printf("  n%-2d %s %.1f%%\n", b.nodeID, bar, pct)
			}
		}
	}

	// Memory.
	fmt.Printf("\n  %s\n\n", cBold("Memory"))
	for _, b := range state.brokers {
		total := b.memAlloc + b.memFree
		pct := float64(0)
		if total > 0 {
			pct = b.memAlloc / total * 100
		}
		bar := horizontalBar(pct, 30)
		pctStr := fmt.Sprintf("%.1f%%", pct)
		if pct > 90 {
			pctStr = cRed(pctStr)
		} else if pct > 75 {
			pctStr = cYellow(pctStr)
		}
		fmt.Printf("  n%-2d %s %s  %s/%s\n",
			b.nodeID, bar, pctStr,
			humanVal(b.memAlloc), humanVal(b.memAlloc+b.memFree))
	}

	// IO ops rate.
	fmt.Printf("\n  %s\n", cBold("IO Ops"))
	if tsR, ok := state.series["io_reads_total"]; ok {
		rates := tsR.rates()
		if len(rates) > 0 {
			last := rates[len(rates)-1]
			fmt.Printf("  Reads   %s  %s\n", sparkline(rates, 25), cCyan(fmt.Sprintf("%.0f/s", last)))
		}
	}
	if tsW, ok := state.series["io_writes_total"]; ok {
		rates := tsW.rates()
		if len(rates) > 0 {
			last := rates[len(rates)-1]
			fmt.Printf("  Writes  %s  %s\n", sparkline(rates, 25), cCyan(fmt.Sprintf("%.0f/s", last)))
		}
	}
}

// horizontalBar renders a colored bar with fill and empty segments.
func horizontalBar(pct float64, width int) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := int(pct / 100 * float64(width))
	empty := width - filled

	var colorFn func(a ...interface{}) string
	switch {
	case pct > 90:
		colorFn = cRed
	case pct > 75:
		colorFn = cYellow
	default:
		colorFn = cGreen
	}

	return colorFn(strings.Repeat("█", filled)) + cDim(strings.Repeat("░", empty))
}

// sparkline renders a single-line Unicode sparkline.
var sparkChars = []rune("▁▂▃▄▅▆▇█")

func sparkline(values []float64, width int) string {
	if len(values) == 0 {
		return ""
	}
	if len(values) > width {
		values = values[len(values)-width:]
	}

	minVal, maxVal := math.Inf(1), math.Inf(-1)
	for _, v := range values {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	spread := maxVal - minVal
	if spread == 0 {
		spread = 1
	}

	var sb strings.Builder
	for _, v := range values {
		idx := int((v - minVal) / spread * float64(len(sparkChars)-1))
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sparkChars) {
			idx = len(sparkChars) - 1
		}
		sb.WriteRune(sparkChars[idx])
	}
	return cGreen(sb.String())
}

// printAreaChart renders a multi-row ASCII area chart.
// Each row represents a band of the value range, filled with block chars.
func printAreaChart(values []float64, height, width int, colorFn func(a ...interface{}) string) {
	if len(values) == 0 {
		return
	}
	if len(values) > width {
		values = values[len(values)-width:]
	}

	minVal, maxVal := math.Inf(1), math.Inf(-1)
	for _, v := range values {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	spread := maxVal - minVal
	if spread == 0 {
		spread = 1
		maxVal = minVal + 1
	}

	// Render top to bottom.
	for row := height - 1; row >= 0; row-- {
		rowMin := minVal + (float64(row)/float64(height))*spread
		rowMax := minVal + (float64(row+1)/float64(height))*spread

		// Y-axis label on the left.
		switch row {
		case height - 1:
			fmt.Printf("  %7s │", humanVal(maxVal))
		case 0:
			fmt.Printf("  %7s │", humanVal(minVal))
		default:
			fmt.Printf("  %7s │", "")
		}

		var line strings.Builder
		for _, v := range values {
			if v >= rowMax {
				line.WriteString("█")
			} else if v > rowMin {
				// Partial fill.
				frac := (v - rowMin) / (rowMax - rowMin)
				idx := int(frac * float64(len(sparkChars)-1))
				if idx < 0 {
					idx = 0
				}
				if idx >= len(sparkChars) {
					idx = len(sparkChars) - 1
				}
				line.WriteRune(sparkChars[idx])
			} else {
				line.WriteString(" ")
			}
		}
		fmt.Println(colorFn(line.String()))
	}
	// X-axis.
	fmt.Printf("  %7s └%s\n", "", strings.Repeat("─", len(values)))
	dur := time.Duration(len(values)*3) * time.Second
	fmt.Printf("  %7s  %s%s\n", "", cDim(fmt.Sprintf("← %s ago", dur)), cDim(strings.Repeat(" ", max(0, len(values)-15))+"now →"))
}

func humanVal(v float64) string {
	abs := math.Abs(v)
	switch {
	case abs >= 1e12:
		return fmt.Sprintf("%.1fT", v/1e12)
	case abs >= 1e9:
		return fmt.Sprintf("%.1fG", v/1e9)
	case abs >= 1e6:
		return fmt.Sprintf("%.1fM", v/1e6)
	case abs >= 1e3:
		return fmt.Sprintf("%.1fK", v/1e3)
	default:
		return fmt.Sprintf("%.1f", v)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
