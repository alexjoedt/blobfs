package blobfs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// inode returns the inode number of the file at path.
func inode(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("inode stat %q: %v", path, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("inode comparison not supported on this platform")
	}
	return stat.Ino
}

// nlink returns the hard-link count for the file at path.
func nlink(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("nlink stat %q: %v", path, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("nlink not supported on this platform")
	}
	return uint64(stat.Nlink)
}

// TestDedup_SameContentSingleInode verifies that two keys with identical content
// share a single inode via the object store.
func TestDedup_SameContentSingleInode(t *testing.T) {
	bs, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	content := "identical content for deduplication"

	if err := bs.Put(ctx, "dedup/key1.txt", strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Put(ctx, "dedup/key2.txt", strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	data1 := filepath.Join(bs.createPathFromKey("dedup/key1.txt"), blobFileName)
	data2 := filepath.Join(bs.createPathFromKey("dedup/key2.txt"), blobFileName)

	ino1 := inode(t, data1)
	ino2 := inode(t, data2)

	if ino1 != ino2 {
		t.Errorf("expected same inode for identical content, got %d and %d", ino1, ino2)
	}

	// The object should have nlink >= 3 (object anchor + 2 refs).
	meta, err := bs.Stat(ctx, "dedup/key1.txt")
	if err != nil {
		t.Fatal(err)
	}
	objPath := bs.objectPath(meta.Sha256)
	if n := nlink(t, objPath); n < 3 {
		t.Errorf("expected nlink >= 3 for shared object, got %d", n)
	}
}

// TestDedup_DifferentContentDifferentInode verifies that blobs with different
// content have distinct inodes.
func TestDedup_DifferentContentDifferentInode(t *testing.T) {
	bs, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := bs.Put(ctx, "unique/a.txt", strings.NewReader("content A")); err != nil {
		t.Fatal(err)
	}
	if err := bs.Put(ctx, "unique/b.txt", strings.NewReader("content B")); err != nil {
		t.Fatal(err)
	}

	data1 := filepath.Join(bs.createPathFromKey("unique/a.txt"), blobFileName)
	data2 := filepath.Join(bs.createPathFromKey("unique/b.txt"), blobFileName)

	if inode(t, data1) == inode(t, data2) {
		t.Error("expected different inodes for different content")
	}
}

// TestDedup_ReadBack verifies that deduplicated blobs can still be read back correctly.
func TestDedup_ReadBack(t *testing.T) {
	bs, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	want := "shared content read-back test"

	_ = bs.Put(ctx, "readback/key1.txt", strings.NewReader(want))
	_ = bs.Put(ctx, "readback/key2.txt", strings.NewReader(want))

	for _, key := range []string{"readback/key1.txt", "readback/key2.txt"} {
		rc, err := bs.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		got, _ := io.ReadAll(rc)
		_ = rc.Close()
		if string(got) != want {
			t.Errorf("key %q: got %q, want %q", key, got, want)
		}
	}
}

// TestDedup_DeleteDoesNotRemoveSharedObject verifies that deleting one key that
// shares content with another key does not make the other key unreadable.
func TestDedup_DeleteDoesNotRemoveSharedObject(t *testing.T) {
	bs, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	content := "shared content to survive partial delete"

	_ = bs.Put(ctx, "shared/keep.txt", strings.NewReader(content))
	_ = bs.Put(ctx, "shared/remove.txt", strings.NewReader(content))

	if err := bs.Delete(ctx, "shared/remove.txt"); err != nil {
		t.Fatal(err)
	}

	rc, err := bs.Get(ctx, "shared/keep.txt")
	if err != nil {
		t.Fatalf("Get after sibling delete: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != content {
		t.Errorf("got %q, want %q", got, content)
	}
}

// TestDedup_ObjectDirCreated verifies that the objects/ directory is always
// created by NewStorage.
func TestDedup_ObjectDirCreated(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(bs.objectsDir); err != nil {
		t.Errorf("objects dir not created: %v", err)
	}
	if _, err := os.Stat(bs.refsDir); err != nil {
		t.Errorf("refs dir not created: %v", err)
	}
}

// TestDedup_Migrate verifies that Migrate populates objects/ and converts existing
// ref data files to hard links.
func TestDedup_Migrate(t *testing.T) {
	dir := t.TempDir()

	// Write two blobs with the old code path by bypassing commitData: we simulate
	// an old-style store by writing plain files directly and then running Migrate.
	bs, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	content := "migrate me"

	// Normal Put goes through commitData and already enters objects/.
	// To simulate a pre-CAS store we write the file ourselves.
	key := "migrate/test.txt"
	storagePath := bs.createPathFromKey(key)
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(storagePath, blobFileName)
	if err := os.WriteFile(dataPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	// Write meta manually so Walk can find it.
	meta := &Meta{
		Key:    key,
		Size:   int64(len(content)),
		Sha256: func() string { b, _ := bs.NewBlob(); _, _ = io.WriteString(b, content); return b.Hash() }(),
	}
	if err := writeTestMeta(filepath.Join(storagePath, metaFileName), meta); err != nil {
		t.Fatal(err)
	}

	// Remove the object if it was created by any stray Put above.
	_ = os.RemoveAll(bs.objectPath(meta.Sha256))

	stats, err := bs.Migrate(ctx)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	if stats.BlobsScanned != 1 {
		t.Errorf("BlobsScanned: got %d, want 1", stats.BlobsScanned)
	}
	if stats.BlobsLinked != 1 {
		t.Errorf("BlobsLinked: got %d, want 1", stats.BlobsLinked)
	}

	// Object must now exist.
	objPath := bs.objectPath(meta.Sha256)
	if _, err := os.Stat(objPath); err != nil {
		t.Errorf("object not created after Migrate: %v", err)
	}

	// Running Migrate again must be idempotent.
	stats2, err := bs.Migrate(ctx)
	if err != nil {
		t.Fatalf("Migrate (idempotent): %v", err)
	}
	if stats2.BlobsSkipped != 1 {
		t.Errorf("expected 1 skipped on re-run, got %d", stats2.BlobsSkipped)
	}
}

// TestGC_RemovesOrphanedObject verifies that GC removes an object whose nlink == 1,
// i.e. all refs have been deleted.
func TestGC_RemovesOrphanedObject(t *testing.T) {
	bs, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	content := "orphan me"

	if err := bs.Put(ctx, "gc/key.txt", strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	meta, err := bs.Stat(ctx, "gc/key.txt")
	if err != nil {
		t.Fatal(err)
	}
	objPath := bs.objectPath(meta.Sha256)

	if err := bs.Delete(ctx, "gc/key.txt"); err != nil {
		t.Fatal(err)
	}

	// Object must still exist before GC.
	if _, err := os.Stat(objPath); err != nil {
		t.Fatalf("object missing before GC: %v", err)
	}

	stats, err := bs.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if stats.ObjectsScanned != 1 {
		t.Errorf("ObjectsScanned: got %d, want 1", stats.ObjectsScanned)
	}
	if stats.ObjectsRemoved != 1 {
		t.Errorf("ObjectsRemoved: got %d, want 1", stats.ObjectsRemoved)
	}
	if stats.BytesReclaimed != int64(len(content)) {
		t.Errorf("BytesReclaimed: got %d, want %d", stats.BytesReclaimed, len(content))
	}

	// Object must be gone after GC.
	if _, err := os.Stat(objPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected object to be removed after GC, stat err: %v", err)
	}
}

// TestGC_PreservesReferencedObject verifies that GC keeps objects that are still
// referenced by at least one ref hard-link.
func TestGC_PreservesReferencedObject(t *testing.T) {
	bs, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	content := "keep me"

	if err := bs.Put(ctx, "gc/keep1.txt", strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Put(ctx, "gc/keep2.txt", strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	meta, _ := bs.Stat(ctx, "gc/keep1.txt")
	objPath := bs.objectPath(meta.Sha256)

	// Delete only one of the two refs.
	if err := bs.Delete(ctx, "gc/keep1.txt"); err != nil {
		t.Fatal(err)
	}

	stats, err := bs.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}

	if stats.ObjectsRemoved != 0 {
		t.Errorf("GC removed a referenced object; ObjectsRemoved=%d", stats.ObjectsRemoved)
	}

	// Object must still be readable via the surviving ref.
	if _, err := os.Stat(objPath); err != nil {
		t.Errorf("object unexpectedly removed: %v", err)
	}
	rc, err := bs.Get(ctx, "gc/keep2.txt")
	if err != nil {
		t.Fatalf("Get after GC: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != content {
		t.Errorf("got %q, want %q", got, content)
	}
}

// TestGC_EmptyStore verifies that GC runs without error on an empty store.
func TestGC_EmptyStore(t *testing.T) {
	bs, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	stats, err := bs.GC(context.Background())
	if err != nil {
		t.Fatalf("GC on empty store: %v", err)
	}
	if stats.ObjectsScanned != 0 || stats.ObjectsRemoved != 0 {
		t.Errorf("unexpected stats on empty store: %+v", stats)
	}
}

// TestGC_MultipleOrphans verifies that GC reclaims all orphaned objects in one pass.
func TestGC_MultipleOrphans(t *testing.T) {
	bs, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	keys := []string{"gc/a.txt", "gc/b.txt", "gc/c.txt"}
	for i, key := range keys {
		if err := bs.Put(ctx, key, strings.NewReader(key+string(rune('0'+i)))); err != nil {
			t.Fatal(err)
		}
		if err := bs.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
	}

	stats, err := bs.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if stats.ObjectsRemoved != len(keys) {
		t.Errorf("ObjectsRemoved: got %d, want %d", stats.ObjectsRemoved, len(keys))
	}
}

// writeTestMeta is a test helper that writes a Meta to a JSON file.
func writeTestMeta(path string, meta *Meta) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(meta)
}
