package tui

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/branchbase/branchbase/internal/config"
	"github.com/branchbase/branchbase/internal/driver"
)

// TestReadKeyDecodesArrowKeys verifies that ANSI escape sequences produced by
// arrow keys are decoded into logical key names, and that ordinary
// characters pass through untouched.
func TestReadKeyDecodesArrowKeys(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"up arrow (CSI)", "\x1b[A", "up"},
		{"down arrow (CSI)", "\x1b[B", "down"},
		{"right arrow (CSI)", "\x1b[C", "right"},
		{"left arrow (CSI)", "\x1b[D", "left"},
		{"up arrow (SS3)", "\x1bOA", "up"},
		{"down arrow (SS3)", "\x1bOB", "down"},
		{"lone escape", "\x1b", "esc"},
		{"ordinary character", "a", "a"},
		{"ordinary character j", "j", "j"},
		{"carriage return", "\r", "enter"},
		{"newline", "\n", "enter"},
		{"ctrl+c", "\x03", "ctrl+c"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reader := bufio.NewReader(strings.NewReader(tc.input))
			got, err := readKey(reader, nil)
			if err != nil {
				t.Fatalf("readKey(%q) returned error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("readKey(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// selectedRowLine returns the first rendered line containing the selection
// cursor and reports whether it also highlights the given branch.
func selectedRowHasBranch(output, branch string) bool {
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "▶") && strings.Contains(line, branch) {
			return true
		}
	}
	return false
}

func newKeyTestDriver() *mockDriver {
	return &mockDriver{
		branches: []driver.BranchInfo{
			{Name: "main", Database: "myapp_dev", IsProtected: true},
			{Name: "feat_a", Database: "myapp_dev_feat_a"},
			{Name: "feat_b", Database: "myapp_dev_feat_b"},
		},
	}
}

// TestRunArrowKeysMoveSelection drives the full event loop with raw arrow-key
// escape sequences and verifies the selection cursor actually moves.
func TestRunArrowKeysMoveSelection(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()

	// Down arrow moves the selection to the second row.
	drv := newKeyTestDriver()
	input := bytes.NewBufferString("\x1b[B" + "q")
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := Run(ctx, t.TempDir(), &cfg, drv, input, &output); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if !selectedRowHasBranch(output.String(), "feat_a") {
		t.Fatalf("down arrow did not move selection to feat_a; output:\n%s", output.String())
	}

	// Down then up returns the selection to the first row.
	drv = newKeyTestDriver()
	input = bytes.NewBufferString("\x1b[B\x1b[B\x1b[A" + "q")
	output.Reset()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := Run(ctx2, t.TempDir(), &cfg, drv, input, &output); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if !selectedRowHasBranch(output.String(), "feat_a") {
		t.Fatalf("up arrow did not move selection back to feat_a; output:\n%s", output.String())
	}
	// After two downs and one up, the final render must select feat_a (index 1).
	lines := strings.Split(output.String(), "\n")
	lastFeatB, lastMain := -1, -1
	for i, line := range lines {
		if strings.Contains(line, "▶") {
			if strings.Contains(line, "feat_b") {
				lastFeatB = i
			}
			if strings.Contains(line, "myapp_dev ") || strings.Contains(line, "main") {
				lastMain = i
			}
		}
	}
	if lastFeatB < 0 {
		t.Fatalf("expected a render with feat_b selected after two downs")
	}
	if lastMain > lastFeatB {
		t.Fatalf("expected final selection to move up from feat_b")
	}
}

// TestRunOrdinaryCharactersStillWork ensures plain key input keeps working
// alongside escape-sequence decoding.
func TestRunOrdinaryCharactersStillWork(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	drv := newKeyTestDriver()

	input := bytes.NewBufferString("j" + "q")
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := Run(ctx, t.TempDir(), &cfg, drv, input, &output); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if !selectedRowHasBranch(output.String(), "feat_a") {
		t.Fatalf("'j' did not move selection to feat_a; output:\n%s", output.String())
	}
}
