package sqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// maxSnapshotAttempts bounds how many times a snapshot is retried while
// concurrent writers keep mutating the source database mid-copy.
const maxSnapshotAttempts = 64

// snapshotSettleDelay paces our own re-reads between attempts. It never
// blocks, pauses, or slows down database writers.
const snapshotSettleDelay = 5 * time.Millisecond

// snapshotDatabase copies the SQLite database file at sourcePath — together
// with its -wal and -shm sidecar files when present — to targetPath.
//
// Writers may keep committing to the source database while the copy runs, and
// a checkpoint may rewrite the main database file or restart the WAL at any
// moment. A naive file copy can therefore capture a torn main file or a WAL
// generation that no longer matches it, silently dropping the most recent
// committed transactions. To guarantee a consistent snapshot without pausing
// writers or locking the database, every attempt is verified afterwards and
// retried until the captured files provably reflect a single point in time.
func snapshotDatabase(ctx context.Context, sourcePath, targetPath string) error {
	for attempt := 0; attempt < maxSnapshotAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(snapshotSettleDelay):
			}
		}

		walCopied, err := copySnapshotFiles(sourcePath, targetPath)
		if err != nil {
			return err
		}

		consistent, err := verifySnapshot(sourcePath, targetPath, walCopied)
		if err != nil {
			return fmt.Errorf("sqlite: failed to verify snapshot of %q: %w", sourcePath, err)
		}
		if consistent {
			return nil
		}
	}
	return fmt.Errorf("sqlite: could not obtain a consistent snapshot of %q after %d attempts", sourcePath, maxSnapshotAttempts)
}

// copySnapshotFiles performs a single snapshot attempt, cloning the main
// database file and any present WAL/SHM sidecars. It reports whether a WAL
// file was captured.
func copySnapshotFiles(sourcePath, targetPath string) (bool, error) {
	// Clean up any stale target database files
	_ = os.Remove(targetPath)
	_ = os.Remove(targetPath + "-wal")
	_ = os.Remove(targetPath + "-shm")

	if err := CloneFile(sourcePath, targetPath); err != nil {
		return false, err
	}

	walCopied := false
	if _, err := os.Stat(sourcePath + "-wal"); err == nil {
		if err := CloneFile(sourcePath+"-wal", targetPath+"-wal"); err != nil {
			// The WAL may be checkpointed and removed between stat and copy;
			// treat that as a failed attempt and let verification retry.
			if !errors.Is(err, os.ErrNotExist) {
				return false, err
			}
		} else {
			walCopied = true
		}
	}
	if _, err := os.Stat(sourcePath + "-shm"); err == nil {
		// The SHM file only caches the wal-index and is rebuilt during
		// recovery; clone it best-effort.
		_ = CloneFile(sourcePath+"-shm", targetPath+"-shm")
	}
	return walCopied, nil
}

// verifySnapshot reports whether the files captured by copySnapshotFiles still
// match the source database, proving that no checkpoint or WAL restart
// interleaved with the copy:
//
//   - the main database file must be byte-identical to the source (no
//     checkpoint wrote pages while or after it was copied), and
//   - the captured WAL must still be a prefix of the source WAL (the WAL was
//     not restarted; appends after the copy are harmless because recovery
//     stops at the first incomplete frame).
func verifySnapshot(sourcePath, targetPath string, walCopied bool) (bool, error) {
	identical, err := filesIdentical(sourcePath, targetPath)
	if err != nil || !identical {
		return false, err
	}

	sourceWalPath := sourcePath + "-wal"
	if walCopied {
		targetWal, err := os.ReadFile(targetPath + "-wal")
		if err != nil {
			return false, err
		}
		sourceWal, err := os.ReadFile(sourceWalPath)
		if err != nil {
			// WAL vanished or was restarted after the copy: retry.
			return false, nil
		}
		if len(targetWal) > len(sourceWal) || !bytes.Equal(sourceWal[:len(targetWal)], targetWal) {
			return false, nil
		}
		return true, nil
	}

	// No WAL was captured; if one exists now, retry so it is included.
	if _, err := os.Stat(sourceWalPath); err == nil {
		return false, nil
	}
	return true, nil
}

// filesIdentical reports whether two files have identical contents.
func filesIdentical(a, b string) (bool, error) {
	fa, err := os.Open(a)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = fa.Close()
	}()
	fb, err := os.Open(b)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = fb.Close()
	}()

	sa, err := fa.Stat()
	if err != nil {
		return false, err
	}
	sb, err := fb.Stat()
	if err != nil {
		return false, err
	}
	if sa.Size() != sb.Size() {
		return false, nil
	}

	bufA := make([]byte, 256*1024)
	bufB := make([]byte, 256*1024)
	for {
		nA, errA := io.ReadFull(fa, bufA)
		nB, errB := io.ReadFull(fb, bufB)
		if nA != nB || !bytes.Equal(bufA[:nA], bufB[:nB]) {
			return false, nil
		}
		if errA == io.EOF || errA == io.ErrUnexpectedEOF {
			return true, nil
		}
		if errA != nil {
			return false, errA
		}
		if errB != nil {
			return false, errB
		}
	}
}
