package sqlite

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"
)

var recordRe = regexp.MustCompile(`R(\d+)\n`)

// readSnapshotRecords extracts the committed record sequence contained in a
// snapshot: records already folded into the main database file, followed by
// records still pending in the WAL sidecar.
func readSnapshotRecords(t *testing.T, mainPath string) []int {
	t.Helper()
	mainBytes, err := os.ReadFile(mainPath)
	if err != nil {
		t.Fatalf("read snapshot main file: %v", err)
	}
	walBytes, _ := os.ReadFile(mainPath + "-wal") // WAL may legitimately be absent
	combined := append(mainBytes, walBytes...)

	matches := recordRe.FindAllStringSubmatch(string(combined), -1)
	ids := make([]int, 0, len(matches))
	for _, m := range matches {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatalf("corrupt record %q in snapshot", m[0])
		}
		ids = append(ids, n)
	}
	return ids
}

// TestCreateBranchSnapshotCompleteWithoutWrites verifies that a snapshot
// taken while no writes are in flight contains every committed record.
func TestCreateBranchSnapshotCompleteWithoutWrites(t *testing.T) {
	tempDir := t.TempDir()
	basePath := filepath.Join(tempDir, "myapp_dev.db")

	if err := os.WriteFile(basePath, []byte("R0\nR1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(basePath+"-wal", []byte("R2\nR3\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	drv, err := New(Config{BasePath: basePath})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := drv.CreateBranch(context.Background(), "main", "quiet"); err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}

	target := filepath.Join(tempDir, "myapp_dev_quiet.db")
	got := readSnapshotRecords(t, target)
	want := []int{0, 1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("snapshot records = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("snapshot records = %v, want %v", got, want)
		}
	}
}

// TestCreateBranchSnapshotConsistentUnderWriteLoad simulates a SQLite
// WAL-mode workload at the file level. A writer continuously commits records
// into the WAL file and periodically checkpoints: the WAL content is folded
// into the main database file at a low offset (like real checkpoints, which
// overwrite database pages a concurrent file copy has typically already
// passed) and the WAL is truncated. The checkpoint is deliberately triggered
// in the middle of every snapshot copy — the exact interleaving that used to
// drop committed records from snapshots.
//
// Every snapshot must reflect a consistent committed state: a contiguous
// record prefix with no gaps, containing at least everything committed
// before the snapshot started.
func TestCreateBranchSnapshotConsistentUnderWriteLoad(t *testing.T) {
	tempDir := t.TempDir()
	basePath := filepath.Join(tempDir, "myapp_dev.db")

	// Pad the main database file so copying it takes long enough for a
	// checkpoint to interleave with the copy.
	padding := bytes.Repeat([]byte("x"), 4*1024*1024)
	if err := os.WriteFile(basePath, padding, 0o644); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	committed := 0
	var folded []byte // checkpointed records, rewritten at offset 0 each checkpoint
	checkpointed := make(map[string]bool)
	stop := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}

			mu.Lock()
			// Commit one record into the WAL.
			if f, err := os.OpenFile(basePath+"-wal", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
				_, _ = fmt.Fprintf(f, "R%d\n", committed)
				_ = f.Close()
			}
			committed++

			// If a snapshot copy of the main file is in flight (its target
			// file just appeared), checkpoint immediately to race with it —
			// once per snapshot target. The checkpoint overwrites the start
			// of the main file, a region the in-flight copy has already
			// read, and truncates the WAL.
			entries, _ := os.ReadDir(tempDir)
			for _, e := range entries {
				name := e.Name()
				if len(name) > len("myapp_dev_snap") && name[:len("myapp_dev_snap")] == "myapp_dev_snap" && !checkpointed[name] {
					checkpointed[name] = true
					wal, _ := os.ReadFile(basePath + "-wal")
					if len(wal) > 0 {
						folded = append(folded, wal...)
						if mf, err := os.OpenFile(basePath, os.O_WRONLY, 0o644); err == nil {
							_, _ = mf.WriteAt(folded, 0)
							_ = mf.Close()
						}
						_ = os.Remove(basePath + "-wal")
					}
				}
			}
			mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
	}()

	drv, err := New(Config{BasePath: basePath})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		branch := fmt.Sprintf("snap%d", i)

		mu.Lock()
		committedBefore := committed
		mu.Unlock()

		if err := drv.CreateBranch(ctx, "main", branch); err != nil {
			t.Fatalf("CreateBranch %q under write load: %v", branch, err)
		}

		target := filepath.Join(tempDir, "myapp_dev_"+branch+".db")
		ids := readSnapshotRecords(t, target)

		// The snapshot must be a contiguous prefix R0..Rk with no gaps or
		// duplicates (a checkpoint racing the copy must not punch a hole).
		for idx, id := range ids {
			if id != idx {
				t.Fatalf("snapshot %q is inconsistent: record at position %d is R%d, want R%d (missing committed changes)", branch, idx, id, idx)
			}
		}
		// Everything committed before the snapshot started must be included.
		if len(ids) < committedBefore {
			t.Fatalf("snapshot %q missing committed records: contains %d, want at least %d", branch, len(ids), committedBefore)
		}
	}
}
