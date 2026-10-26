package file

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

func assertLastGoodFileBackup(t *testing.T, c *Client, expectedID string, expected []byte) {
	t.Helper()
	md, err := c.CurrentMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if md == nil || md.ID != expectedID {
		t.Fatalf("metadata = %+v, want ID %s", md, expectedID)
	}
	b, err := os.ReadFile(md.Name)
	if err != nil {
		t.Fatalf("last good backup %q missing: %v", md.Name, err)
	}
	if !bytes.Equal(b, expected) {
		t.Fatalf("last good backup content = %q, want %q", b, expected)
	}
}

func TestUploadMetadataRenameFailurePreservesLastGoodBackup(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(t.TempDir(), "backup.sqlite", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("good-backup")), "old"); err != nil {
		t.Fatal(err)
	}

	failure := errors.New("injected metadata rename failure")
	c.renameFile = func(src, dst string) error {
		if dst == c.metaPath {
			return failure
		}
		return fsutil.Rename(src, dst)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("replacement")), "new"); !errors.Is(err, failure) {
		t.Fatalf("upload failure = %v, want injected error", err)
	}
	assertLastGoodFileBackup(t, c, "old", []byte("good-backup"))
	// A separate client startup must see the previous completed backup.
	restarted, err := NewClient(c.dir, c.name, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertLastGoodFileBackup(t, restarted, "old", []byte("good-backup"))
}

func TestUploadRecoveryAfterCrashBetweenDataAndMetadata(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(t.TempDir(), "backup.sqlite", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("last-complete")), "old"); err != nil {
		t.Fatal(err)
	}

	c.renameFile = func(src, dst string) error {
		if dst == c.metaPath {
			panic("simulated process crash after data rename")
		}
		return fsutil.Rename(src, dst)
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("fault injector did not simulate a crash")
			}
		}()
		_ = c.Upload(ctx, bytes.NewReader([]byte("crashed-new-backup")), "new")
	}()
	restarted, err := NewClient(c.dir, c.name, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertLastGoodFileBackup(t, restarted, "old", []byte("last-complete"))
}

func TestUploadRollbackSameSecondTimestampCollision(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(t.TempDir(), "backup.sqlite", &Options{Timestamp: true})
	if err != nil {
		t.Fatal(err)
	}
	fixed := time.Date(2026, 10, 25, 20, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return fixed }
	if err := c.Upload(ctx, bytes.NewReader([]byte("timestamp-old")), "old"); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected metadata rename failure")
	c.renameFile = func(src, dst string) error {
		if dst == c.metaPath {
			return failure
		}
		return fsutil.Rename(src, dst)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("same-second")), "new"); !errors.Is(err, failure) {
		t.Fatalf("upload returned %v, want metadata failure", err)
	}
	assertLastGoodFileBackup(t, c, "old", []byte("timestamp-old"))
}

func TestUploadMetadataRenameFailureWithoutPriorBackup(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(t.TempDir(), "backup.sqlite", nil)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("metadata rename blocked")
	c.renameFile = func(src, dst string) error {
		if dst == c.metaPath {
			return failure
		}
		return fsutil.Rename(src, dst)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("unpublished")), "new"); !errors.Is(err, failure) {
		t.Fatalf("Upload error %v, want %v", err, failure)
	}
	if _, err := os.Stat(filepath.Join(c.dir, "backup.sqlite")); !os.IsNotExist(err) {
		t.Fatalf("uncommitted initial backup remains visible: %v", err)
	}
	if id, err := c.CurrentID(ctx); err != nil || id != "" {
		t.Fatalf("CurrentID = %q, %v; want empty", id, err)
	}
}

func TestUploadRecoveryAfterCrashBeforeDataRename(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(t.TempDir(), "backup.sqlite", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("previous-valid")), "old"); err != nil {
		t.Fatal(err)
	}

	finalPath := filepath.Join(c.dir, "backup.sqlite")
	c.renameFile = func(src, dst string) error {
		if dst == finalPath {
			panic("simulated crash before data rename")
		}
		return fsutil.Rename(src, dst)
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("did not inject crash")
			}
		}()
		_ = c.Upload(ctx, bytes.NewReader([]byte("not-published")), "new")
	}()
	restarted, err := NewClient(c.dir, c.name, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertLastGoodFileBackup(t, restarted, "old", []byte("previous-valid"))
}

func TestUploadRecoveryAfterCrashAfterMetadataCommit(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(t.TempDir(), "backup.sqlite", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("previous-valid")), "old"); err != nil {
		t.Fatal(err)
	}

	c.renameFile = func(src, dst string) error {
		if dst == c.metaPath {
			if err := fsutil.Rename(src, dst); err != nil {
				return err
			}
			panic("simulated crash after committed metadata rename")
		}
		return fsutil.Rename(src, dst)
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("did not inject crash")
			}
		}()
		_ = c.Upload(ctx, bytes.NewReader([]byte("committed-new")), "new")
	}()
	restarted, err := NewClient(c.dir, c.name, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertLastGoodFileBackup(t, restarted, "new", []byte("committed-new"))
	// A fully committed restart should remove stale recovery artifacts.
	if _, err := os.Stat(restarted.pendingPath()); !os.IsNotExist(err) {
		t.Fatalf("pending journal remains after committed restart: %v", err)
	}
	if _, err := os.Stat(restarted.rollbackPath()); !os.IsNotExist(err) {
		t.Fatalf("rollback file remains after committed restart: %v", err)
	}
}

func TestUploadSameIDMetadataRollbackUsesGeneration(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(t.TempDir(), "backup.sqlite", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("good")), "same-id"); err != nil {
		t.Fatal(err)
	}
	old, err := c.CurrentMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}

	failure := errors.New("metadata rename failed for reused identifier")
	c.renameFile = func(src, dst string) error {
		if dst == c.metaPath {
			return failure
		}
		return fsutil.Rename(src, dst)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("bad")), "same-id"); !errors.Is(err, failure) {
		t.Fatalf("Upload error = %v, want failure", err)
	}
	newMetadata, err := c.CurrentMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if old.Generation != newMetadata.Generation {
		t.Fatalf("previous backup generation changed on rollback")
	}
	assertLastGoodFileBackup(t, c, "same-id", []byte("good"))
}

func TestUploadRecoveryClearsOrphanPrejournalHardLink(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(t.TempDir(), "backup.sqlite", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("prior")), "old"); err != nil {
		t.Fatal(err)
	}

	// A power loss after preserving the old inode but before publishing
	// the journal leaves an unneeded rollback link and the old stable file.
	if err := os.Link(filepath.Join(c.dir, c.name), c.rollbackPath()); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	restarted, err := NewClient(c.dir, c.name, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertLastGoodFileBackup(t, restarted, "old", []byte("prior"))
	if err := restarted.Upload(ctx, bytes.NewReader([]byte("next")), "new"); err != nil {
		t.Fatalf("orphan link blocked next upload: %v", err)
	}
	assertLastGoodFileBackup(t, restarted, "new", []byte("next"))
}

func TestUploadRecoveryRepeatsAfterInterruptedRollback(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(t.TempDir(), "backup.sqlite", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("prior-valid")), "old"); err != nil {
		t.Fatal(err)
	}

	dataPath := filepath.Join(c.dir, c.name)
	failure := errors.New("reject updated metadata")
	c.renameFile = func(src, dst string) error {
		if dst == c.metaPath {
			return failure
		}
		if src == c.metaPath+".restore" && dst == dataPath {
			if err := fsutil.Rename(src, dst); err != nil {
				return err
			}
			panic("crash after restoring old data but before deleting journal")
		}
		return fsutil.Rename(src, dst)
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("failed to inject rollback-recovery crash")
			}
		}()
		_ = c.Upload(ctx, bytes.NewReader([]byte("uncommitted-new")), "new")
	}()

	// Recovery must be idempotent: the rollback link remains available
	// if the process stopped after restoring the old inode to its path.
	restarted, err := NewClient(c.dir, c.name, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertLastGoodFileBackup(t, restarted, "old", []byte("prior-valid"))
	if err := restarted.Upload(ctx, bytes.NewReader([]byte("later")), "later"); err != nil {
		t.Fatalf("new upload after interrupted rollback failed: %v", err)
	}
	assertLastGoodFileBackup(t, restarted, "later", []byte("later"))
}
