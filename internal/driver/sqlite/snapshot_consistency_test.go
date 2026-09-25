package sqlite

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestSnapshotCompleteWithoutWrites verifies a snapshot taken on a quiet
// database copies the main file and WAL sidecar verbatim.
func TestSnapshotCompleteWithoutWrites(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	basePath := filepath.Join(tempDir, "myapp_dev.db")
	dbContent := []byte("SQLite format 3\x00main-database-pages")
	walContent := []byte("wal-frames-with-latest-commits")

	if err := os.WriteFile(basePath, dbContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(basePath+"-wal", walContent, 0o644); err != nil {
		t.Fatal(err)
	}

	drv, err := New(Config{BasePath: basePath})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := drv.CreateBranch(context.Background(), "main", "quiet"); err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}

	targetPath := filepath.Join(tempDir, "myapp_dev_quiet.db")
	gotDB, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("target db missing: %v", err)
	}
	if string(gotDB) != string(dbContent) {
		t.Fatalf("target db = %q, want %q", gotDB, dbContent)
	}
	gotWal, err := os.ReadFile(targetPath + "-wal")
	if err != nil {
		t.Fatalf("target wal missing: %v", err)
	}
	if string(gotWal) != string(walContent) {
		t.Fatalf("target wal = %q, want %q", gotWal, walContent)
	}
}

// TestVerifySnapshotDetectsInconsistency unit-tests the consistency verifier
// against torn main-file copies and restarted WALs.
func TestVerifySnapshotDetectsInconsistency(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) (string, string) {
		dir := t.TempDir()
		src := filepath.Join(dir, "src.db")
		dst := filepath.Join(dir, "dst.db")
		if err := os.WriteFile(src, []byte("db-pages-v1"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, []byte("db-pages-v1"), 0o644); err != nil {
			t.Fatal(err)
		}
		return src, dst
	}

	t.Run("consistent without wal", func(t *testing.T) {
		t.Parallel()
		src, dst := setup(t)
		ok, err := verifySnapshot(src, dst, false)
		if err != nil || !ok {
			t.Fatalf("verifySnapshot = %v, %v; want true, nil", ok, err)
		}
	})

	t.Run("torn main file", func(t *testing.T) {
		t.Parallel()
		src, dst := setup(t)
		if err := os.WriteFile(dst, []byte("db-pages-TORN"), 0o644); err != nil {
			t.Fatal(err)
		}
		ok, err := verifySnapshot(src, dst, false)
		if err != nil || ok {
			t.Fatalf("verifySnapshot = %v, %v; want false, nil", ok, err)
		}
	})

	t.Run("main file changed after copy", func(t *testing.T) {
		t.Parallel()
		src, dst := setup(t)
		if err := os.WriteFile(src, []byte("db-pages-v2-checkpointed"), 0o644); err != nil {
			t.Fatal(err)
		}
		ok, err := verifySnapshot(src, dst, false)
		if err != nil || ok {
			t.Fatalf("verifySnapshot = %v, %v; want false, nil", ok, err)
		}
	})

	t.Run("consistent wal prefix", func(t *testing.T) {
		t.Parallel()
		src, dst := setup(t)
		if err := os.WriteFile(src+"-wal", []byte("frame1frame2frame3"), 0o644); err != nil {
			t.Fatal(err)
		}
		// Target captured an earlier, shorter prefix of the same WAL.
		if err := os.WriteFile(dst+"-wal", []byte("frame1frame2"), 0o644); err != nil {
			t.Fatal(err)
		}
		ok, err := verifySnapshot(src, dst, true)
		if err != nil || !ok {
			t.Fatalf("verifySnapshot = %v, %v; want true, nil", ok, err)
		}
	})

	t.Run("wal restarted after copy", func(t *testing.T) {
		t.Parallel()
		src, dst := setup(t)
		// Source WAL is a new generation that no longer contains the copied frames.
		if err := os.WriteFile(src+"-wal", []byte("salt2-frame"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst+"-wal", []byte("salt1-frame1"), 0o644); err != nil {
			t.Fatal(err)
		}
		ok, err := verifySnapshot(src, dst, true)
		if err != nil || ok {
			t.Fatalf("verifySnapshot = %v, %v; want false, nil", ok, err)
		}
	})

	t.Run("wal vanished after copy", func(t *testing.T) {
		t.Parallel()
		src, dst := setup(t)
		if err := os.WriteFile(dst+"-wal", []byte("stale-frames"), 0o644); err != nil {
			t.Fatal(err)
		}
		ok, err := verifySnapshot(src, dst, true)
		if err != nil || ok {
			t.Fatalf("verifySnapshot = %v, %v; want false, nil", ok, err)
		}
	})

	t.Run("wal appeared during copy", func(t *testing.T) {
		t.Parallel()
		src, dst := setup(t)
		if err := os.WriteFile(src+"-wal", []byte("new-wal"), 0o644); err != nil {
			t.Fatal(err)
		}
		ok, err := verifySnapshot(src, dst, false)
		if err != nil || ok {
			t.Fatalf("verifySnapshot = %v, %v; want false, nil", ok, err)
		}
	})
}

// TestSnapshotConsistentUnderContinuousWrites hammers the source database with
// commits and simulated checkpoints while repeatedly snapshotting. Every
// snapshot must reflect a consistent committed state: the records captured in
// the main file plus its WAL must form a contiguous prefix of all committed
// records, and everything committed before the snapshot started must be present.
func TestSnapshotConsistentUnderContinuousWrites(t *testing.T) {
	t.Parallel()

	recordPadding := strings.Repeat("x", 1016)

	tempDir := t.TempDir()
	basePath := filepath.Join(tempDir, "myapp_dev.db")
	if err := os.WriteFile(basePath, []byte("DB\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	drv, err := New(Config{BasePath: basePath})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	ctx := context.Background()

	var committed atomic.Int64
	stop := make(chan struct{})
	writerDone := make(chan struct{})

	// Writer goroutine simulates a SQLite client: each record is a committed
	// transaction appended to the WAL; every 50 records a checkpoint folds the
	// WAL into the main database file and restarts it.
	go func() {
		defer close(writerDone)
		id := 0
		for {
			select {
			case <-stop:
				return
			default:
			}

			// 1KiB per committed record so that copying the database and WAL
			// takes long enough for checkpoints to interleave with snapshots.
			rec := fmt.Sprintf("R%06d\n", id) + recordPadding
			f, err := os.OpenFile(basePath+"-wal", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				return
			}
			if _, err := f.WriteString(rec); err != nil {
				_ = f.Close()
				return
			}
			_ = f.Close()
			id++
			committed.Store(int64(id))

			if id%200 == 0 {
				wal, err := os.ReadFile(basePath + "-wal")
				if err == nil {
					df, err := os.OpenFile(basePath, os.O_APPEND|os.O_WRONLY, 0o644)
					if err == nil {
						_, _ = df.Write(wal)
						_ = df.Close()
					}
					_ = os.Remove(basePath + "-wal")
				}
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()

	recordRe := regexp.MustCompile(`R(\d{6})\n`)
	collectRecords := func(branch string) map[int]bool {
		seen := make(map[int]bool)
		targetPath := filepath.Join(tempDir, "myapp_dev_"+branch+".db")
		for _, p := range []string{targetPath, targetPath + "-wal"} {
			data, err := os.ReadFile(p)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				t.Fatalf("read %s: %v", p, err)
			}
			for _, m := range recordRe.FindAllSubmatch(data, -1) {
				var id int
				if _, err := fmt.Sscanf(string(m[1]), "%d", &id); err == nil {
					seen[id] = true
				}
			}
		}
		return seen
	}

	assertSnapshot := func(branch string, mustContainUpTo int64) {
		seen := collectRecords(branch)
		maxID := -1
		for id := range seen {
			if id > maxID {
				maxID = id
			}
		}
		for i := 0; i <= maxID; i++ {
			if !seen[i] {
				t.Fatalf("snapshot %q is torn: record %d missing below max record %d", branch, i, maxID)
			}
		}
		for i := int64(0); i < mustContainUpTo; i++ {
			if !seen[int(i)] {
				t.Fatalf("snapshot %q lost committed record %d (committed before snapshot started)", branch, i)
			}
		}
	}

	// Keep snapshotting while the writer races ahead, until enough commits
	// and checkpoints have interleaved with the snapshots.
	deadline := time.Now().Add(10 * time.Second)
	var snapshots int
	for i := 0; (i < 200 || committed.Load() < 3000) && time.Now().Before(deadline); i++ {
		branch := fmt.Sprintf("snap_%d", i)
		before := committed.Load()
		if err := drv.CreateBranch(ctx, "main", branch); err != nil {
			t.Fatalf("CreateBranch %q under write load: %v", branch, err)
		}
		assertSnapshot(branch, before)
		_ = drv.DeleteBranch(ctx, branch)
		snapshots++
	}
	t.Logf("took %d consistent snapshots while writer committed %d records", snapshots, committed.Load())

	// Quiesce the writer; a final snapshot must contain every committed record.
	close(stop)
	<-writerDone
	final := committed.Load()
	if final == 0 {
		t.Fatal("writer made no progress")
	}
	if err := drv.CreateBranch(ctx, "main", "snap_final"); err != nil {
		t.Fatalf("final CreateBranch: %v", err)
	}
	assertSnapshot("snap_final", final)
}

// TestSnapshotRetriesUntilConsistent forces the source database to change in
// the middle of the first few snapshot attempts and verifies the snapshot
// mechanism retries until it captures a consistent, complete copy.
func TestSnapshotRetriesUntilConsistent(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "myapp_dev.db")
	targetPath := filepath.Join(tempDir, "myapp_dev_retry.db")

	// Large enough that copy + verification takes milliseconds, so the churn
	// below deterministically invalidates every attempt while it is active.
	content := make([]byte, 4*1024*1024)
	for i := range content {
		content[i] = 'a'
	}
	if err := os.WriteFile(sourcePath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Churn: flip individual bytes in the source far faster than a single
	// snapshot attempt takes, simulating a database under constant writes.
	churnDone := make(chan struct{})
	mutatorDone := make(chan struct{})
	go func() {
		defer close(mutatorDone)
		f, err := os.OpenFile(sourcePath, os.O_RDWR, 0o644)
		if err != nil {
			return
		}
		defer func() { _ = f.Close() }()
		buf := []byte{'b'}
		for offset := int64(0); ; offset = (offset + 4096) % int64(len(content)) {
			select {
			case <-churnDone:
				return
			default:
			}
			if _, err := f.WriteAt(buf, offset); err != nil {
				return
			}
			time.Sleep(20 * time.Microsecond)
		}
	}()

	// Keep the churn active long enough to span many snapshot attempts.
	time.Sleep(100 * time.Millisecond)
	snapshotDone := make(chan error, 1)
	go func() {
		snapshotDone <- snapshotDatabase(context.Background(), sourcePath, targetPath)
	}()
	time.Sleep(100 * time.Millisecond)
	close(churnDone)
	<-mutatorDone

	err := <-snapshotDone
	if err != nil {
		t.Fatalf("snapshotDatabase failed: %v", err)
	}

	got, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	final, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(final) {
		t.Fatalf("snapshot content diverges from the final committed source state")
	}
}
