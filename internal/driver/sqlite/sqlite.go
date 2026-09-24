package sqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/branchbase/branchbase/internal/driver"
	"github.com/branchbase/branchbase/internal/git"
)

// Config holds connection and path parameters for SQLite
type Config struct {
	BasePath      string // Path to primary database file (e.g. "myapp_dev.db" or "./data/app.db")
	DefaultBranch string
}

// SqliteDriver manages filesystem-level database branching for SQLite engines
type SqliteDriver struct {
	cfg Config
	mu  sync.RWMutex
}

func init() {
	driver.Register("sqlite", func(params map[string]interface{}) (driver.Driver, error) {
		cfg := Config{
			BasePath: "myapp_dev.db",
		}

		if p, ok := params["path"].(string); ok && p != "" {
			cfg.BasePath = p
		}
		if base, ok := params["base_database"].(string); ok && base != "" {
			if cfg.BasePath == "myapp_dev.db" {
				cfg.BasePath = base + ".db"
			}
		}
		if def, ok := params["default_branch"].(string); ok && strings.TrimSpace(def) != "" {
			cfg.DefaultBranch = def
		}

		return New(cfg)
	})
}

// New creates a new SqliteDriver instance
func New(cfg Config) (*SqliteDriver, error) {
	if cfg.BasePath == "" {
		return nil, errors.New("sqlite: base_path cannot be empty")
	}
	return &SqliteDriver{cfg: cfg}, nil
}

func (d *SqliteDriver) Name() string {
	return "sqlite"
}

// Ping checks whether the SQLite database file or its parent directory is accessible
func (d *SqliteDriver) Ping(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	d.mu.RLock()
	basePath := d.cfg.BasePath
	d.mu.RUnlock()

	dir := filepath.Dir(basePath)
	if dir != "" && dir != "." {
		if dirInfo, err := os.Stat(dir); err != nil {
			return fmt.Errorf("sqlite base directory %q inaccessible: %w", dir, err)
		} else if !dirInfo.IsDir() {
			return fmt.Errorf("sqlite base directory %q is not a directory", dir)
		}
	}

	if info, err := os.Stat(basePath); err == nil {
		if info.IsDir() {
			return fmt.Errorf("sqlite base path %q is a directory, expected database file", basePath)
		}
		// Verify readable
		f, err := os.Open(basePath)
		if err != nil {
			return fmt.Errorf("sqlite database file %q unreadable: %w", basePath, err)
		}
		_ = f.Close()
	}

	return nil
}

// BranchExists checks whether a database file exists for the given branch
func (d *SqliteDriver) BranchExists(ctx context.Context, branchName string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	targetPath := d.formatDBPath(branchName)
	info, err := os.Stat(targetPath)
	if err == nil {
		return !info.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// maxSnapshotAttempts bounds how many times CreateBranch retries when the
// source database is modified (committed writes or a WAL checkpoint) while
// its files are being copied.
const maxSnapshotAttempts = 10

// sourceFingerprint captures enough state of the source database file to
// detect any committed modification: size, modification time, and the first
// 100 bytes (the SQLite database header, which embeds the file change
// counter that SQLite increments on every commit touching the main file).
type sourceFingerprint struct {
	size    int64
	modTime time.Time
	header  []byte
	exists  bool
}

func fingerprintSource(path string) (sourceFingerprint, error) {
	var fp sourceFingerprint
	info, err := os.Stat(path)
	if err != nil {
		return fp, err
	}
	fp.size = info.Size()
	fp.modTime = info.ModTime()
	fp.exists = true

	f, err := os.Open(path)
	if err != nil {
		return fp, err
	}
	defer func() {
		_ = f.Close()
	}()
	fp.header = make([]byte, 100)
	n, _ := f.Read(fp.header)
	fp.header = fp.header[:n]
	return fp, nil
}

func (fp sourceFingerprint) equal(other sourceFingerprint) bool {
	if !fp.exists || !other.exists {
		return fp.exists == other.exists
	}
	if fp.size != other.size || !fp.modTime.Equal(other.modTime) {
		return false
	}
	if len(fp.header) != len(other.header) {
		return false
	}
	for i := range fp.header {
		if fp.header[i] != other.header[i] {
			return false
		}
	}
	return true
}

// CreateBranch clones sourceBranch into targetBranch using Copy-on-Write / fast streaming.
//
// Writers are never paused or locked out. Instead, the snapshot is taken
// optimistically: the main database file is copied first, then the WAL/SHM
// sidecars, so the copied WAL always covers at least the frames missing from
// the copied main file. If the source main file was modified meanwhile —
// e.g. a WAL checkpoint folding committed frames back into it — the copied
// pair would be inconsistent (committed changes could be lost), so the copy
// is discarded and retried.
func (d *SqliteDriver) CreateBranch(ctx context.Context, sourceBranch, targetBranch string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	sourcePath := d.formatDBPath(sourceBranch)
	targetPath := d.formatDBPath(targetBranch)

	if sourcePath == targetPath {
		return fmt.Errorf("source and target branch database paths are identical: %q", sourcePath)
	}

	srcInfo, err := os.Stat(sourcePath)
	if err != nil {
		return fmt.Errorf("cannot create branch: source database %q does not exist: %w", sourcePath, err)
	}
	if srcInfo.IsDir() {
		return fmt.Errorf("source database %q is a directory", sourcePath)
	}

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return fmt.Errorf("failed to create destination directory for %q: %w", targetPath, err)
	}

	var lastErr error
	for attempt := 0; attempt < maxSnapshotAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		before, err := fingerprintSource(sourcePath)
		if err != nil {
			return fmt.Errorf("cannot snapshot source database %q: %w", sourcePath, err)
		}

		// Clean up any stale target database files
		_ = os.Remove(targetPath)
		_ = os.Remove(targetPath + "-wal")
		_ = os.Remove(targetPath + "-shm")

		// Snapshot primary database file
		if err := CloneFile(sourcePath, targetPath); err != nil {
			return fmt.Errorf("failed to snapshot sqlite database from %q to %q: %w", sourcePath, targetPath, err)
		}

		// Also clone WAL and SHM files if present for consistent crash-recovery state
		if _, err := os.Stat(sourcePath + "-wal"); err == nil {
			_ = CloneFile(sourcePath+"-wal", targetPath+"-wal")
		}
		if _, err := os.Stat(sourcePath + "-shm"); err == nil {
			_ = CloneFile(sourcePath+"-shm", targetPath+"-shm")
		}

		after, err := fingerprintSource(sourcePath)
		if err != nil {
			return fmt.Errorf("cannot verify source database %q after snapshot: %w", sourcePath, err)
		}
		if before.equal(after) {
			// The source main file was untouched while we copied: the
			// (main, WAL) pair is a consistent point-in-time snapshot.
			return nil
		}

		// A committed write or WAL checkpoint raced with the copy; the
		// snapshot may be missing committed changes. Discard and retry.
		lastErr = fmt.Errorf("source database %q changed during snapshot (attempt %d/%d)", sourcePath, attempt+1, maxSnapshotAttempts)
		_ = os.Remove(targetPath)
		_ = os.Remove(targetPath + "-wal")
		_ = os.Remove(targetPath + "-shm")
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}

	return fmt.Errorf("failed to obtain a consistent snapshot of %q: %w", sourcePath, lastErr)
}

// DeleteBranch destroys an ephemeral SQLite database branch along with WAL/SHM artifacts
func (d *SqliteDriver) DeleteBranch(ctx context.Context, branchName string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	sanitized := git.SanitizeBranchName(branchName)
	if d.isBaseBranch(sanitized) {
		return fmt.Errorf("cannot delete protected base database %q", d.cfg.BasePath)
	}

	targetPath := d.formatDBPath(branchName)
	if targetPath == d.cfg.BasePath {
		return fmt.Errorf("cannot delete protected base database %q", d.cfg.BasePath)
	}

	if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete branch database %q: %w", targetPath, err)
	}
	_ = os.Remove(targetPath + "-wal")
	_ = os.Remove(targetPath + "-shm")

	return nil
}

// ListBranches returns all SQLite database files matching the base filename prefix in the same directory
func (d *SqliteDriver) ListBranches(ctx context.Context) ([]driver.BranchInfo, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	d.mu.RLock()
	basePath := d.cfg.BasePath
	d.mu.RUnlock()

	dir := filepath.Dir(basePath)
	if dir == "" {
		dir = "."
	}
	baseFile := filepath.Base(basePath)
	ext := filepath.Ext(baseFile)
	basePrefix := strings.TrimSuffix(baseFile, ext)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read sqlite directory %q: %w", dir, err)
	}

	var branches []driver.BranchInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		// Exclude SQLite journal, WAL, and SHM sidecar files
		if strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-shm") || strings.HasSuffix(name, "-journal") {
			continue
		}

		var branchName string
		var isProtected bool

		if name == baseFile {
			branchName = d.listedDefaultBranch()
			isProtected = true
		} else if strings.HasPrefix(name, basePrefix+"_") && strings.HasSuffix(name, ext) {
			branchName = strings.TrimPrefix(name, basePrefix+"_")
			branchName = strings.TrimSuffix(branchName, ext)
			isProtected = false
		} else {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		size := info.Size()
		// Include WAL file size if it exists
		if walInfo, err := os.Stat(filepath.Join(dir, name+"-wal")); err == nil {
			size += walInfo.Size()
		}

		branches = append(branches, driver.BranchInfo{
			Name:        branchName,
			Database:    name,
			CreatedAt:   info.ModTime(),
			SizeBytes:   size,
			IsActive:    false,
			IsProtected: isProtected,
		})
	}

	return branches, nil
}

// Close releases any held resources for the SQLite driver (no-op for file-based SQLite)
func (d *SqliteDriver) Close() error {
	return nil
}

// formatDBPath converts a branch name into the corresponding SQLite database file path
func (d *SqliteDriver) formatDBPath(branch string) string {
	sanitized := git.SanitizeBranchName(branch)
	if d.isBaseBranch(sanitized) {
		return d.cfg.BasePath
	}

	dir := filepath.Dir(d.cfg.BasePath)
	baseFile := filepath.Base(d.cfg.BasePath)
	ext := filepath.Ext(baseFile)
	basePrefix := strings.TrimSuffix(baseFile, ext)

	targetFile := fmt.Sprintf("%s_%s%s", basePrefix, sanitized, ext)
	if dir == "" || dir == "." {
		return targetFile
	}
	return filepath.Join(dir, targetFile)
}

func (d *SqliteDriver) isBaseBranch(sanitized string) bool {
	def := strings.TrimSpace(d.cfg.DefaultBranch)
	if def == "" {
		return sanitized == "main" || sanitized == "master" || sanitized == "default"
	}
	return sanitized == git.SanitizeBranchName(def)
}

func (d *SqliteDriver) listedDefaultBranch() string {
	def := strings.TrimSpace(d.cfg.DefaultBranch)
	if def == "" {
		return "main"
	}
	return git.SanitizeBranchName(def)
}
