package tui

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/branchbase/branchbase/internal/config"
	"github.com/branchbase/branchbase/internal/driver"
)

// dripReader yields input one chunk at a time with a delay, simulating a real
// terminal where the bytes of an escape sequence arrive in separate reads.
type dripReader struct {
	chunks [][]byte
	delay  time.Duration
}

func (d *dripReader) Read(p []byte) (int, error) {
	if len(d.chunks) == 0 {
		return 0, io.EOF
	}
	time.Sleep(d.delay)
	n := copy(p, d.chunks[0])
	if n == len(d.chunks[0]) {
		d.chunks = d.chunks[1:]
	} else {
		d.chunks[0] = d.chunks[0][n:]
	}
	return n, nil
}

func TestReadKeyDecodesArrowSequences(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"up", "\x1b[A", "up"},
		{"down", "\x1b[B", "down"},
		{"right", "\x1b[C", "right"},
		{"left", "\x1b[D", "left"},
		{"up application mode", "\x1bOA", "up"},
		{"down application mode", "\x1bOB", "down"},
		{"plain character", "j", "j"},
		{"enter", "\r", "enter"},
		{"ctrl+c", "\x03", "ctrl+c"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			key, err := newTerminalKeyReader(strings.NewReader(tc.input)).readKey()
			if err != nil {
				t.Fatalf("readKey(%q) error: %v", tc.input, err)
			}
			if key != tc.want {
				t.Fatalf("readKey(%q) = %q, want %q", tc.input, key, tc.want)
			}
		})
	}
}

func TestReadKeyArrowSequenceSplitAcrossReads(t *testing.T) {
	t.Parallel()

	// The bytes of ESC [ B arrive one per read, as on a real terminal.
	in := &dripReader{chunks: [][]byte{[]byte("\x1b"), []byte("["), []byte("B")}, delay: 2 * time.Millisecond}
	key, err := newTerminalKeyReader(in).readKey()
	if err != nil {
		t.Fatalf("readKey error: %v", err)
	}
	if key != "down" {
		t.Fatalf("readKey split sequence = %q, want %q", key, "down")
	}
}

func TestReadKeyLoneEscape(t *testing.T) {
	t.Parallel()

	key, err := newTerminalKeyReader(strings.NewReader("\x1b")).readKey()
	if err != nil {
		t.Fatalf("readKey error: %v", err)
	}
	if key != "esc" {
		t.Fatalf("readKey(lone ESC) = %q, want %q", key, "esc")
	}
}

// lastScreen returns the final rendered frame of the TUI output.
func lastScreen(t *testing.T, output string) string {
	t.Helper()
	frames := strings.Split(output, ClearScreen)
	for i := len(frames) - 1; i >= 0; i-- {
		if strings.TrimSpace(frames[i]) != "" {
			return frames[i]
		}
	}
	t.Fatal("no rendered frame found in TUI output")
	return ""
}

func selectedRowBranch(t *testing.T, frame string) string {
	t.Helper()
	for _, line := range strings.Split(frame, "\n") {
		if strings.Contains(line, SelectedBg) {
			return line
		}
	}
	t.Fatalf("no selected row found in frame:\n%s", frame)
	return ""
}

func runTUIWithInput(t *testing.T, branches []driver.BranchInfo, in io.Reader) string {
	t.Helper()
	drv := &mockDriver{branches: branches}
	cfg := config.DefaultConfig()
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := Run(ctx, t.TempDir(), &cfg, drv, in, &output); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	return output.String()
}

func TestRunArrowKeysMoveSelection(t *testing.T) {
	t.Parallel()

	branches := []driver.BranchInfo{
		{Name: "main", Database: "myapp_dev", IsProtected: true},
		{Name: "feature_x", Database: "myapp_dev_feature_x"},
		{Name: "feature_y", Database: "myapp_dev_feature_y"},
	}

	// Arrow down twice, arrow up once, then quit: selection must end on the
	// second row, and the raw escape bytes must not be treated as quit.
	in := bytes.NewBufferString("\x1b[B\x1b[B\x1b[Aq")
	out := runTUIWithInput(t, branches, in)

	row := selectedRowBranch(t, lastScreen(t, out))
	if !strings.Contains(row, "feature_x") {
		t.Fatalf("selected row = %q, want it to contain feature_x", row)
	}
}

func TestRunArrowKeysSplitAcrossReads(t *testing.T) {
	t.Parallel()

	branches := []driver.BranchInfo{
		{Name: "main", Database: "myapp_dev", IsProtected: true},
		{Name: "feature_x", Database: "myapp_dev_feature_x"},
	}

	// ESC and "[B" arrive in separate reads, then q quits. A lone-ESC
	// misparse would quit the dashboard before the selection moves.
	in := &dripReader{chunks: [][]byte{[]byte("\x1b"), []byte("[B"), []byte("q")}, delay: 5 * time.Millisecond}
	out := runTUIWithInput(t, branches, in)

	row := selectedRowBranch(t, lastScreen(t, out))
	if !strings.Contains(row, "feature_x") {
		t.Fatalf("selected row = %q, want it to contain feature_x", row)
	}
}

func TestRunPlainCharacterInputStillWorks(t *testing.T) {
	t.Parallel()

	branches := []driver.BranchInfo{
		{Name: "main", Database: "myapp_dev", IsProtected: true},
		{Name: "feature_x", Database: "myapp_dev_feature_x"},
	}

	in := bytes.NewBufferString("jjkq")
	out := runTUIWithInput(t, branches, in)

	row := selectedRowBranch(t, lastScreen(t, out))
	if !strings.Contains(row, "main") {
		t.Fatalf("selected row = %q, want it to contain main", row)
	}
}
