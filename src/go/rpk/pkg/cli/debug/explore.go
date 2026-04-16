// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package debug

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/cobra"
)

func NewExploreCommand() *cobra.Command {
	var aiDir string
	var noDashboard bool
	var yolo bool
	cmd := &cobra.Command{
		Use:   "explore",
		Short: "Launch an AI-powered cluster diagnostic session",
		Long: `Launch an AI-powered diagnostic session with a live cluster health
dashboard and Claude Code AI assistant in split panes.

The top pane shows a live-refreshing cluster health dashboard with broker
status, leaderless partitions, disk usage, and critical signals. The bottom
pane runs Claude Code with Redpanda-specific diagnostic skills.

Requires tmux for split-pane mode. Falls back to Claude Code only if tmux
is not available.

Requires:
  - Claude Code CLI installed (https://docs.anthropic.com/en/docs/claude-code)
  - ANTHROPIC_API_KEY environment variable set
  - tmux (optional, for split-pane dashboard)
`,
		Example: `
Start an interactive diagnostic session with live dashboard:
  rpk debug explore

Start with a specific question:
  rpk debug explore "my decommission has been stuck for 2 hours"

Skip the dashboard, use Claude Code only:
  rpk debug explore --no-dashboard
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			claudePath, err := exec.LookPath("claude")
			if err != nil {
				out.Die("claude code CLI not found in PATH, install: npm install -g @anthropic-ai/claude-code")
			}

			resolvedAIDir := resolveAIDir(aiDir)
			if _, err := os.Stat(resolvedAIDir); os.IsNotExist(err) {
				out.Die("ai skills directory not found at %q, use --ai-dir to specify the path", resolvedAIDir)
			}

			rpkPath, _ := os.Executable()
			tmuxPath, tmuxErr := exec.LookPath("tmux")

			if !noDashboard && tmuxErr == nil {
				launchWithTmux(tmuxPath, rpkPath, claudePath, resolvedAIDir, args, yolo)
			} else {
				if tmuxErr != nil && !noDashboard {
					fmt.Println("tmux not found — launching without dashboard. Install tmux for split-pane mode.")
					fmt.Println()
				}
				launchClaudeOnly(claudePath, resolvedAIDir, args, yolo)
			}
		},
	}
	cmd.Flags().StringVar(&aiDir, "ai-dir", "", "Path to the AI skills directory (default: <rpk-binary-dir>/ai)")
	cmd.Flags().BoolVar(&noDashboard, "no-dashboard", false, "Skip the live dashboard, launch Claude Code only")
	cmd.Flags().BoolVar(&yolo, "yolo", false, "Pass --dangerously-skip-permissions to Claude Code (for demos; skips all permission prompts)")
	return cmd
}

// launchWithTmux creates a tmux session with four panes.
func launchWithTmux(tmuxPath, rpkPath, claudePath, aiDir string, args []string, yolo bool) {
	sessionName := "rpk-explore"

	tmuxRun(tmuxPath, "kill-session", "-t", sessionName)

	claudeCmd := fmt.Sprintf("cd %s && %s", aiDir, claudePath)
	if yolo {
		claudeCmd += " --dangerously-skip-permissions"
	}
	if len(args) > 0 {
		claudeCmd += fmt.Sprintf(" %q", args[0])
	}
	dashboardCmd := fmt.Sprintf("%s debug explore-dashboard", rpkPath)
	chartCmd := fmt.Sprintf("%s debug explore-chart", rpkPath)
	eventTailCmd := fmt.Sprintf("%s debug explore-event-tail", rpkPath)

	newSession := tmuxCmd(tmuxPath, "new-session", "-d", "-s", sessionName, "-x", "200", "-y", "50", claudeCmd)
	if err := newSession.Run(); err != nil {
		out.Die("failed to create tmux session: %v", err)
	}

	tmuxRun(tmuxPath, "split-window", "-t", sessionName+":0.0", "-v", "-b", "-p", "40", eventTailCmd)
	tmuxRun(tmuxPath, "split-window", "-t", sessionName+":0.0", "-v", "-b", "-p", "60", dashboardCmd)
	tmuxRun(tmuxPath, "split-window", "-t", sessionName+":0.0", "-h", "-p", "50", chartCmd)
	tmuxRun(tmuxPath, "select-pane", "-t", sessionName+":0.3")

	attach := tmuxCmd(tmuxPath, "attach-session", "-t", sessionName)
	attach.Stdin = os.Stdin
	attach.Stdout = os.Stdout
	attach.Stderr = os.Stderr

	if err := attach.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
	}

	tmuxRun(tmuxPath, "kill-session", "-t", sessionName)
}

func tmuxCmd(tmuxPath string, args ...string) *exec.Cmd {
	c := exec.CommandContext(context.Background(), tmuxPath, args...) //nolint:gosec // args are controlled
	c.Env = os.Environ()
	return c
}

func tmuxRun(tmuxPath string, args ...string) {
	_ = tmuxCmd(tmuxPath, args...).Run()
}

func launchClaudeOnly(claudePath, aiDir string, args []string, yolo bool) {
	var claudeArgs []string
	if yolo {
		claudeArgs = append(claudeArgs, "--dangerously-skip-permissions")
	}
	if len(args) > 0 {
		claudeArgs = append(claudeArgs, args[0])
	}

	c := exec.CommandContext(context.Background(), claudePath, claudeArgs...) //nolint:gosec // args are user input
	c.Dir = aiDir
	c.Stdin = os.Stdin
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr

	fmt.Println("Launching Claude Code with Redpanda diagnostic skills...")
	fmt.Printf("Skills directory: %s\n\n", aiDir)

	if err := c.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
		out.Die("failed to run Claude Code: %v", err)
	}
}

func resolveAIDir(explicit string) string {
	if explicit != "" {
		return explicit
	}
	ex, err := os.Executable()
	if err != nil {
		return "ai"
	}
	ex, err = filepath.EvalSymlinks(ex)
	if err != nil {
		return "ai"
	}
	dir := filepath.Dir(ex)
	candidate := filepath.Join(dir, "ai")
	if _, err := os.Stat(candidate); err == nil {
		return candidate
	}
	if runtime.GOOS != "windows" {
		srcCandidate := filepath.Join(dir, "..", "ai")
		if abs, err := filepath.Abs(srcCandidate); err == nil {
			if _, err := os.Stat(abs); err == nil {
				return abs
			}
		}
	}
	return filepath.Join(dir, "ai")
}
