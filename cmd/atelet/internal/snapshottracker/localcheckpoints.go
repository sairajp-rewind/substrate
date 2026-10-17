// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshottracker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/imagecache"
)

// PruneLocalCheckpoints removes every local snapshot of the actor. A missing
// directory is not an error, so retries are safe.
func PruneLocalCheckpoints(ctx context.Context, actorUID string) error {
	return pruneLocalCheckpointDir(ctx, ateletpath.LocalCheckpointsDir(actorUID))
}

func pruneLocalCheckpointDir(ctx context.Context, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("while listing local checkpoints in %s: %w", dir, err)
	}
	// Every entry is attempted: one undeletable snapshot must not strand the
	// others on disk.
	var errs []error
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			errs = append(errs, fmt.Errorf("while pruning local checkpoint %s: %w", path, err))
			continue
		}
		slog.InfoContext(ctx, "pruned local checkpoint", slog.String("path", path))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if err := os.Remove(dir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("while removing local checkpoints dir %s: %w", dir, err)
	}
	return nil
}

// ResetDir empties dir with remove and recreates it with mode. what names the
// directory in errors.
func ResetDir(what, dir string, remove func(string) error, mode os.FileMode) error {
	if err := remove(dir); err != nil {
		return fmt.Errorf("while deleting %s: %w", what, err)
	}
	if err := os.MkdirAll(dir, mode); err != nil {
		return fmt.Errorf("while creating %s: %w", what, err)
	}
	return nil
}

// removeEmptyVolumeDirs removes each empty mountpoint directory under the
// actor's external volumes directory, refusing to delete through a populated
// directory in case an unmount failed.
func removeEmptyVolumeDirs(actorUID string) error {
	volumesDir := ateletpath.ExternalVolumeMountsDir(actorUID)
	entries, err := os.ReadDir(volumesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("while reading volumes dir: %w", err)
	}
	for _, entry := range entries {
		volPath := filepath.Join(volumesDir, entry.Name())
		if err := os.Remove(volPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("while removing volume dir: %w", err)
		}
	}
	return nil
}

// ResetActorDirs empties the actor's directories and leaves them in place for
// its next activation. Use RemoveActorDirs when the actor will not come back.
func ResetActorDirs(actorUID string) error {
	// Explicitly leave runsc logs dir untouched.

	// Do not call RemoveAll on volume directories in case the unmount failed:
	// check them first so a still-mounted volume aborts before wiping anything.
	if err := removeEmptyVolumeDirs(actorUID); err != nil {
		return err
	}
	if err := os.MkdirAll(ateletpath.ExternalVolumeMountsDir(actorUID), 0o755); err != nil {
		return fmt.Errorf("while creating volumes dir: %w", err)
	}

	// RemoveAllWritable, not os.RemoveAll: the bundle's upper dir can hold
	// copied-up actor-image directories keeping the image's (possibly
	// read-only) modes, which atelet can't remove as plain root without first
	// making them writable. (The rootfs itself is just an empty mountpoint
	// here: the overlay is mounted in the worker pod's mount namespace, not
	// atelet's, and is detached by worker at teardown.)
	if err := ResetDir("bundle dir", ateletpath.OCIBundleDir(actorUID), imagecache.RemoveAllWritable, 0o700); err != nil {
		return err
	}
	if err := ResetDir("checkpoint-state dir", ateletpath.CheckpointStateDir(actorUID), os.RemoveAll, 0o700); err != nil {
		return err
	}
	if err := ResetDir("restore-state dir", ateletpath.RestoreStateDir(actorUID), os.RemoveAll, 0o700); err != nil {
		return err
	}
	if err := ResetDir("durable-dir volumes mount dir", ateletpath.DurableDirVolumeMountsDir(actorUID), os.RemoveAll, 0o755); err != nil {
		return err
	}
	// World-readable (0o755): bind-mounted read-only into the actor, whose
	// workload reads it through the gofer.
	if err := ResetDir("system-info volume roots dir", ateletpath.SystemInfoVolumeRootsDir(actorUID), os.RemoveAll, 0o755); err != nil {
		return err
	}

	return nil
}

// RemoveActorDirs reclaims the actor's whole directory tree, root included:
// nothing else on the node deletes it, and no later activation will look here.
//
// It refuses to proceed while a volume directory is still populated, so a
// failed unmount cannot become a deletion of the mount's contents, removes a
// bundle upper dir carrying an image's read-only modes without recreating
// directories, and removes the snapshot state file last so a restart after an
// interrupted cleanup still queues the actor and finishes removing the tree.
func RemoveActorDirs(actorUID string) error {
	if err := removeEmptyVolumeDirs(actorUID); err != nil {
		return err
	}
	actorDir := ateletpath.ActorPath(actorUID)
	stateFile := filepath.Base(ateletpath.ActorSnapshotStateFile(actorUID))
	entries, err := os.ReadDir(actorDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("while reading actor dir: %w", err)
	}
	for _, entry := range entries {
		if entry.Name() == stateFile {
			continue
		}
		if err := imagecache.RemoveAllWritable(filepath.Join(actorDir, entry.Name())); err != nil {
			return fmt.Errorf("while deleting actor dir: %w", err)
		}
	}
	if err := os.RemoveAll(actorDir); err != nil {
		return fmt.Errorf("while deleting actor dir: %w", err)
	}
	return nil
}
