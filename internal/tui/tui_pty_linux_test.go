//go:build linux

package tui

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/branchbase/branchbase/internal/config"
)

const (
	tiocsptrlck = 0x40045431 // TIOCSPTLCK: unlock the slave pseudo-terminal
	tiocgptn    = 0x80045430 // TIOCGPTN: get slave pseudo-terminal number
)

// openPTY allocates a pseudo-terminal pair, returning master and slave ends.
func openPTY(t *testing.T) (master, slave *os.File) {
	t.Helper()

	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Skipf("pseudo-terminals unavailable: %v", err)
	}

	var unlock int32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(), tiocsptrlck, uintptr(unsafe.Pointer(&unlock))); errno != 0 {
		_ = master.Close()
		t.Skipf("cannot unlock pseudo-terminal: %v", errno)
	}

	var n uint32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(), tiocgptn, uintptr(unsafe.Pointer(&n))); errno != 0 {
		_ = master.Close()
		t.Skipf("cannot query pseudo-terminal number: %v", errno)
	}

	slave, err = os.OpenFile(fmt.Sprintf("/dev/pts/%d", n), os.O_RDWR, 0)
	if err != nil {
		_ = master.Close()
		t.Skipf("cannot open slave pseudo-terminal: %v", err)
	}
	return master, slave
}

// TestRunArrowKeysOnRealTerminal runs the event loop against an actual
// pseudo-terminal. Without cbreak mode the terminal line-buffers and echoes
// input, so the arrow-key sequence would neither reach the program nor move
// the cursor (it would be echoed onto the screen instead).
func TestRunArrowKeysOnRealTerminal(t *testing.T) {
	t.Parallel()

	master, slave := openPTY(t)
	defer func() { _ = master.Close() }()
	defer func() { _ = slave.Close() }()

	cfg := config.DefaultConfig()
	drv := newKeyTestDriver()

	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, t.TempDir(), &cfg, drv, slave, &output)
	}()

	// Give the event loop a moment to switch the terminal into cbreak mode.
	time.Sleep(200 * time.Millisecond)

	// Press the down arrow, then quit. No newline is ever sent: input must be
	// delivered key-by-key, not line-buffered.
	if _, err := master.Write([]byte("\x1b[B")); err != nil {
		t.Fatalf("failed to write arrow key to terminal: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if _, err := master.Write([]byte("q")); err != nil {
		t.Fatalf("failed to write quit key to terminal: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("event loop did not process terminal input without a newline (input stuck in canonical line buffer)")
	}

	if !selectedRowHasBranch(output.String(), "feat_a") {
		t.Fatalf("down arrow on real terminal did not move selection to feat_a; output:\n%s", output.String())
	}
}
