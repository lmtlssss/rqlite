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
	if err != nil { t.Fatal(err) }
	if err := c.Upload(ctx, bytes.NewReader([]byte("good-backup")), "old"); err != nil { t.Fatal(err) }

	failure := errors.New("injected metadata rename failure")
	c.renameFile = func(src, dst string) error {
		if dst == c.metaPath { return failure }
		return fsutil.Rename(src, dst)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("replacement")), "new"); !errors.Is(err, failure) {
		t.Fatalf("upload failure = %v, want injected error", err)
	}
	assertLastGoodFileBackup(t, c, "old", []byte("good-backup"))
	// A separate client startup must see the previous completed backup.
	restarted, err := NewClient(c.dir, c.name, nil)
	if err != nil { t.Fatal(err) }
	assertLastGoodFileBackup(t, restarted, "old", []byte("good-backup"))
}

func TestUploadRecoveryAfterCrashBetweenDataAndMetadata(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(t.TempDir(), "backup.sqlite", nil)
	if err != nil { t.Fatal(err) }
	if err := c.Upload(ctx, bytes.NewReader([]byte("last-complete")), "old"); err != nil { t.Fatal(err) }

	c.renameFile = func(src, dst string) error {
		if dst == c.metaPath { panic("simulated process crash after data rename") }
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
	if err != nil { t.Fatal(err) }
	assertLastGoodFileBackup(t, restarted, "old", []byte("last-complete"))
}

func TestUploadRollbackSameSecondTimestampCollision(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(t.TempDir(), "backup.sqlite", &Options{Timestamp:true})
	if err != nil { t.Fatal(err) }
	fixed := time.Date(2026, 10, 25, 20, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return fixed }
	if err := c.Upload(ctx, bytes.NewReader([]byte("timestamp-old")), "old"); err != nil { t.Fatal(err) }
	failure := errors.New("injected metadata rename failure")
	c.renameFile = func(src, dst string) error {
		if dst == c.metaPath { return failure }
		return fsutil.Rename(src,dst)
	}
	if err := c.Upload(ctx, bytes.NewReader([]byte("same-second")), "new"); !errors.Is(err, failure) {
		t.Fatalf("upload returned %v, want metadata failure", err)
	}
	assertLastGoodFileBackup(t, c, "old", []byte("timestamp-old"))
}

func TestUploadMetadataRenameFailureWithoutPriorBackup(t *testing.T) {
	ctx:= context.Background()
	c, err := NewClient(t.TempDir(), "backup.sqlite", nil)
	if err != nil { t.Fatal(err) }
	failure:= errors.New("metadata rename blocked")
	c.renameFile = func(src,dst string) error {
		if dst == c.metaPath { return failure }
		return fsutil.Rename(src,dst)
	}
	if err:=c.Upload(ctx,bytes.NewReader([]byte("unpublished")),"new"); !errors.Is(err,failure) {
		t.Fatalf("Upload error %v, want %v",err,failure)
	}
	if _,err:=os.Stat(filepath.Join(c.dir,"backup.sqlite")); !os.IsNotExist(err) {
		t.Fatalf("uncommitted initial backup remains visible: %v",err)
	}
	if id,err:=c.CurrentID(ctx);err!=nil || id!="" {
		t.Fatalf("CurrentID = %q, %v; want empty",id,err)
	}
}
