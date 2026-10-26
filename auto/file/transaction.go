package file

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// pendingUpload records which data file and metadata generation must become
// durable together. Both paths are local to the configured backup directory.
type pendingUpload struct {
	Target     string `json:"target"`
	Generation string `json:"generation"`
	HadPrior   bool   `json:"had_prior"`
}

func newGeneration() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("create backup generation: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

func (c *Client) pendingPath() string {
	return c.metaPath + ".pending"
}

func (c *Client) rollbackPath() string {
	return c.metaPath + ".rollback"
}

// recoverPending completes a committed upload or rolls back an incomplete one.
// The new metadata generation is the commit marker. Call during startup and
// before another upload; a previous crash can happen after either rename.
func (c *Client) recoverPending() error {
	raw, err := os.ReadFile(c.pendingPath())
	if os.IsNotExist(err) {
		// An interrupted hard-link reservation can leave an orphan rollback
		// link before its journal is installed. In that phase no new payload
		// has been published. Remove the link only if its inode still matches
		// the currently completed backup; otherwise fail closed.
		rollbackInfo, statErr := os.Lstat(c.rollbackPath())
		if os.IsNotExist(statErr) {
			return nil
		}
		if statErr != nil {
			return fmt.Errorf("inspect orphan local backup link: %w", statErr)
		}
		meta, readErr := os.ReadFile(c.metaPath)
		if readErr != nil {
			return fmt.Errorf("cannot safely resolve orphan local backup link: %w", readErr)
		}
		var current Metadata
		if err := json.Unmarshal(meta, &current); err != nil {
			return fmt.Errorf("read completed metadata for orphan recovery: %w", err)
		}
		if filepath.Dir(current.Name) != c.dir {
			return fmt.Errorf("orphan local backup has unexpected metadata target")
		}
		currentInfo, statErr := os.Lstat(current.Name)
		if statErr != nil {
			return fmt.Errorf("inspect completed backup for orphan recovery: %w", statErr)
		}
		if !os.SameFile(rollbackInfo, currentInfo) {
			return fmt.Errorf("orphan rollback differs from completed backup; refusing cleanup")
		}
		if err := fsutil.Remove(c.rollbackPath()); err != nil {
			return fmt.Errorf("remove verified orphan backup link: %w", err)
		}
		return fsutil.SyncDirMaybe(c.dir)
	}
	if err != nil {
		return fmt.Errorf("read pending local backup: %w", err)
	}

	var p pendingUpload
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("parse pending local backup: %w", err)
	}
	if p.Generation == "" || filepath.Dir(p.Target) != c.dir {
		return fmt.Errorf("invalid pending local backup target or generation")
	}

	// A current metadata record carrying this unique generation makes the
	// transaction committed. Otherwise the former completed file wins.
	var current Metadata
	meta, err := os.ReadFile(c.metaPath)
	if err == nil {
		if err := json.Unmarshal(meta, &current); err != nil {
			return fmt.Errorf("parse current backup metadata during recovery: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("read backup metadata during recovery: %w", err)
	}
	committed := current.Generation == p.Generation && current.Name == p.Target
	if committed {
		// The new metadata is the commit marker. Release the old inode
		// before the journal; after a crash, redoing this cleanup is safe.
		if err := fsutil.Remove(c.rollbackPath()); err != nil {
			return fmt.Errorf("remove committed backup rollback link: %w", err)
		}
		if err := fsutil.SyncDirMaybe(c.dir); err != nil {
			return fmt.Errorf("sync committed backup directory: %w", err)
		}
		if err := fsutil.Remove(c.pendingPath()); err != nil {
			return fmt.Errorf("remove committed backup marker: %w", err)
		}
		return fsutil.SyncDirMaybe(c.dir)
	}

	if p.HadPrior {
		// Keep the original rollback hardlink until AFTER recovery and
		// deletion of the journal are durable. If we crash after putting
		// the old data back, this function can safely repeat the restore.
		oldInfo, err := os.Lstat(c.rollbackPath())
		if err != nil {
			return fmt.Errorf("missing prior backup during recovery: %w", err)
		}
		restorePath := c.metaPath + ".restore"
		if info, err := os.Lstat(restorePath); err == nil {
			// A previous recovery may have stopped after staging this link.
			if !os.SameFile(oldInfo, info) {
				return fmt.Errorf("staged recovery file differs from prior backup")
			}
		} else if os.IsNotExist(err) {
			if err := os.Link(c.rollbackPath(), restorePath); err != nil {
				return fmt.Errorf("stage prior backup recovery: %w", err)
			}
		} else {
			return fmt.Errorf("inspect staged backup recovery: %w", err)
		}
		if err := c.rename(restorePath, p.Target); err != nil {
			return fmt.Errorf("restore last completed local backup: %w", err)
		}
	} else if err := fsutil.Remove(p.Target); err != nil {
		return fmt.Errorf("remove uncommitted local backup: %w", err)
	}
	if err := fsutil.SyncDirMaybe(c.dir); err != nil {
		return fmt.Errorf("sync recovered local backup data: %w", err)
	}

	// For a rollback, remove the marker BEFORE the hardlink: a crash after
	// removing the marker leaves an orphan hardlink to the restored inode,
	// which startup can verify and clean without risking the old backup.
	if err := fsutil.Remove(c.pendingPath()); err != nil {
		return fmt.Errorf("remove rolled-back backup marker: %w", err)
	}
	if err := fsutil.SyncDirMaybe(c.dir); err != nil {
		return fmt.Errorf("sync rolled-back backup marker removal: %w", err)
	}
	if err := fsutil.Remove(c.rollbackPath()); err != nil {
		return fmt.Errorf("remove completed backup rollback link: %w", err)
	}
	return fsutil.SyncDirMaybe(c.dir)
}

// preparePending preserves any prior stable-name backup before the new data
// file replaces it. The pending marker itself is published only after it
// has been fully written and synced.
func (c *Client) preparePending(target, generation string) (retErr error) {
	p := pendingUpload{Target: target, Generation: generation}
	if _, err := os.Lstat(target); err == nil {
		if err := os.Link(target, c.rollbackPath()); err != nil {
			return fmt.Errorf("preserve previous local backup: %w", err)
		}
		p.HadPrior = true
		if err := fsutil.SyncDirMaybe(c.dir); err != nil {
			_ = fsutil.Remove(c.rollbackPath())
			return fmt.Errorf("sync previous local backup link: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect prior local backup: %w", err)
	}

	// Remove the rollback link if the marker never became visible. After
	// publishing the marker, its recovery state owns all cleanup.
	markerPublished := false
	defer func() {
		if retErr != nil && !markerPublished && p.HadPrior {
			_ = fsutil.Remove(c.rollbackPath())
		}
	}()

	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(c.dir, ".backup-marker-*")
	if err != nil {
		return fmt.Errorf("create temporary backup marker: %w", err)
	}
	defer fsutil.Remove(f.Name())
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("write temporary backup marker: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync temporary backup marker: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temporary backup marker: %w", err)
	}

	// Exclusive link avoids replacing another incomplete transaction marker.
	if err := os.Link(f.Name(), c.pendingPath()); err != nil {
		return fmt.Errorf("publish pending backup marker: %w", err)
	}
	markerPublished = true
	if err := fsutil.SyncDirMaybe(c.dir); err != nil {
		return fmt.Errorf("sync pending backup marker directory: %w", err)
	}
	return nil
}

// finishPendingAfterError recovers the prior backup if metadata was not
// committed. Retain the marker and join errors if rollback itself fails.
func (c *Client) finishPendingAfterError(primary error) error {
	if recoverErr := c.recoverPending(); recoverErr != nil {
		return errors.Join(primary, recoverErr)
	}
	return primary
}
