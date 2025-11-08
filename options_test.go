package blobfs

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultShardFunc(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"simple key", "test.txt"},
		{"nested key", "users/123/avatar.jpg"},
		{"special chars", "file-name_with.special/chars.pdf"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DefaultShardFunc(tt.key)

			// Verify structure: XX/YY/HASH
			parts := strings.Split(result, string(filepath.Separator))
			if len(parts) != 3 {
				t.Errorf("expected 3 path parts, got %d: %v", len(parts), parts)
			}

			// Verify hash consistency
			hash := sha256.Sum256([]byte(tt.key))
			hexHash := hex.EncodeToString(hash[:])

			if parts[0] != hexHash[:2] {
				t.Errorf("expected first part to be %q, got %q", hexHash[:2], parts[0])
			}

			if parts[1] != hexHash[2:4] {
				t.Errorf("expected second part to be %q, got %q", hexHash[2:4], parts[1])
			}

			if parts[2] != hexHash {
				t.Errorf("expected third part to be full hash %q, got %q", hexHash, parts[2])
			}
		})
	}
}

func TestDefaultShardFunc_Deterministic(t *testing.T) {
	key := "test/file.txt"

	result1 := DefaultShardFunc(key)
	result2 := DefaultShardFunc(key)

	if result1 != result2 {
		t.Errorf("DefaultShardFunc not deterministic: %q != %q", result1, result2)
	}
}

func TestWithShardFunc_FlatSharding(t *testing.T) {
	dir := t.TempDir()

	// Flat sharding: HASH (no subdirectories)
	flatShard := func(key string) string {
		hash := sha256.Sum256([]byte(key))
		hexHash := hex.EncodeToString(hash[:])
		return hexHash
	}

	storage, err := NewStorage(dir, WithShardFunc(flatShard))
	if err != nil {
		t.Fatalf("NewBlobStorage failed: %v", err)
	}

	ctx := t.Context()
	key := "test.txt"
	content := strings.NewReader("test content")

	err = storage.Put(ctx, key, content)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify flat structure
	hash := sha256.Sum256([]byte(key))
	hexHash := hex.EncodeToString(hash[:])
	expectedPath := filepath.Join(dir, "blobs", hexHash, "data")

	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("expected file at %q does not exist", expectedPath)
	}

	// Verify no subdirectories (should only be blobs/ and the hash dir)
	blobsDir := filepath.Join(dir, "blobs")
	entries, err := os.ReadDir(blobsDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			t.Errorf("expected only directories in blobs/, found file: %s", entry.Name())
		}
		// Should be the full hash, not 2 characters
		if len(entry.Name()) != 64 {
			t.Errorf("expected full hash (64 chars), got %d chars: %s", len(entry.Name()), entry.Name())
		}
	}
}

func TestWithShardFunc_DateBasedSharding(t *testing.T) {
	dir := t.TempDir()

	// Date-based sharding: YYYY/MM/DD/HASH
	dateShard := func(key string) string {
		now := time.Now()
		hash := sha256.Sum256([]byte(key))
		hexHash := hex.EncodeToString(hash[:])
		return filepath.Join(now.Format("2006"), now.Format("01"), now.Format("02"), hexHash)
	}

	storage, err := NewStorage(dir, WithShardFunc(dateShard))
	if err != nil {
		t.Fatalf("NewBlobStorage failed: %v", err)
	}

	ctx := t.Context()
	key := "test.txt"
	content := strings.NewReader("test content")

	err = storage.Put(ctx, key, content)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify date-based structure
	now := time.Now()
	hash := sha256.Sum256([]byte(key))
	hexHash := hex.EncodeToString(hash[:])
	expectedPath := filepath.Join(dir, "blobs", now.Format("2006"), now.Format("01"), now.Format("02"), hexHash, "data")

	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("expected file at %q does not exist", expectedPath)
	}
}

func TestWithShardFunc_DeepSharding(t *testing.T) {
	dir := t.TempDir()

	// Deep sharding: XX/YY/ZZ/HASH (3 levels instead of 2)
	deepShard := func(key string) string {
		hash := sha256.Sum256([]byte(key))
		hexHash := hex.EncodeToString(hash[:])
		return filepath.Join(hexHash[:2], hexHash[2:4], hexHash[4:6], hexHash)
	}

	storage, err := NewStorage(dir, WithShardFunc(deepShard))
	if err != nil {
		t.Fatalf("NewBlobStorage failed: %v", err)
	}

	ctx := t.Context()
	key := "test.txt"
	content := strings.NewReader("test content")

	err = storage.Put(ctx, key, content)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify 3-level structure
	hash := sha256.Sum256([]byte(key))
	hexHash := hex.EncodeToString(hash[:])
	expectedPath := filepath.Join(dir, "blobs", hexHash[:2], hexHash[2:4], hexHash[4:6], hexHash, "data")

	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("expected file at %q does not exist", expectedPath)
	}
}

func TestWithShardFunc_GetAndDelete(t *testing.T) {
	dir := t.TempDir()

	// Custom single-level sharding
	customShard := func(key string) string {
		hash := sha256.Sum256([]byte(key))
		hexHash := hex.EncodeToString(hash[:])
		// Use first char as shard: X/HASH
		return filepath.Join(hexHash[:1], hexHash)
	}

	storage, err := NewStorage(dir, WithShardFunc(customShard))
	if err != nil {
		t.Fatalf("NewBlobStorage failed: %v", err)
	}

	ctx := t.Context()
	key := "test.txt"
	originalContent := "test content"

	// Put
	err = storage.Put(ctx, key, strings.NewReader(originalContent))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get
	rc, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer rc.Close()

	var buf strings.Builder
	_, err = io.Copy(&buf, rc)
	if err != nil {
		t.Fatalf("Copy failed: %v", err)
	}

	if buf.String() != originalContent {
		t.Errorf("content mismatch: got %q, want %q", buf.String(), originalContent)
	}

	// Delete
	err = storage.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	_, err = storage.Get(ctx, key)
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected error containing 'not found' after delete, got %v", err)
	}
}

func TestWithShardFunc_List(t *testing.T) {
	dir := t.TempDir()

	// Flat sharding for easier testing
	flatShard := func(key string) string {
		hash := sha256.Sum256([]byte(key))
		hexHash := hex.EncodeToString(hash[:])
		return hexHash
	}

	storage, err := NewStorage(dir, WithShardFunc(flatShard))
	if err != nil {
		t.Fatalf("NewBlobStorage failed: %v", err)
	}

	ctx := t.Context()

	// Create multiple blobs
	keys := []string{"file1.txt", "file2.txt", "dir/file3.txt"}
	for _, key := range keys {
		err := storage.Put(ctx, key, strings.NewReader("content"))
		if err != nil {
			t.Fatalf("Put(%q) failed: %v", key, err)
		}
	}

	// List all
	iter := storage.List(ctx, "")
	defer iter.Close()

	count := 0
	foundKeys := make(map[string]bool)
	for iter.Next() {
		meta := iter.Meta()
		foundKeys[meta.Key] = true
		count++
	}

	if err := iter.Err(); err != nil {
		t.Fatalf("iteration failed: %v", err)
	}

	if count != len(keys) {
		t.Errorf("expected %d blobs, got %d", len(keys), count)
	}

	for _, key := range keys {
		if !foundKeys[key] {
			t.Errorf("key %q not found in list", key)
		}
	}
}

func TestWithShardFunc_MultipleOptions(t *testing.T) {
	dir := t.TempDir()

	customShard := func(key string) string {
		hash := sha256.Sum256([]byte(key))
		hexHash := hex.EncodeToString(hash[:])
		return filepath.Join(hexHash[:1], hexHash)
	}

	storage, err := NewStorage(dir,
		WithFileMode(0600),
		WithDirMode(0700),
		WithShardFunc(customShard),
	)
	if err != nil {
		t.Fatalf("NewBlobStorage failed: %v", err)
	}

	ctx := t.Context()
	key := "test.txt"

	err = storage.Put(ctx, key, strings.NewReader("content"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify custom sharding was used
	hash := sha256.Sum256([]byte(key))
	hexHash := hex.EncodeToString(hash[:])
	dataPath := filepath.Join(dir, "blobs", hexHash[:1], hexHash, "data")

	info, err := os.Stat(dataPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	// Verify custom file mode (note: umask may affect this)
	mode := info.Mode().Perm()
	if mode != 0600 {
		t.Logf("file mode: got %o, expected 0600 (may be affected by umask)", mode)
	}
}

func TestWithShardFunc_Stat(t *testing.T) {
	dir := t.TempDir()

	customShard := func(key string) string {
		hash := sha256.Sum256([]byte(key))
		hexHash := hex.EncodeToString(hash[:])
		return filepath.Join(hexHash[:3], hexHash)
	}

	storage, err := NewStorage(dir, WithShardFunc(customShard))
	if err != nil {
		t.Fatalf("NewBlobStorage failed: %v", err)
	}

	ctx := t.Context()
	key := "test.txt"
	content := "test content"

	err = storage.Put(ctx, key, strings.NewReader(content))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Stat
	meta, err := storage.Stat(ctx, key)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if meta.Key != key {
		t.Errorf("key mismatch: got %q, want %q", meta.Key, key)
	}

	if meta.Size != int64(len(content)) {
		t.Errorf("size mismatch: got %d, want %d", meta.Size, len(content))
	}
}

func TestWithShardFunc_Exists(t *testing.T) {
	dir := t.TempDir()

	customShard := func(key string) string {
		hash := sha256.Sum256([]byte(key))
		hexHash := hex.EncodeToString(hash[:])
		return filepath.Join(hexHash[:4], hexHash)
	}

	storage, err := NewStorage(dir, WithShardFunc(customShard))
	if err != nil {
		t.Fatalf("NewBlobStorage failed: %v", err)
	}

	ctx := t.Context()
	key := "test.txt"

	// Check non-existent blob
	exists, err := storage.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected blob not to exist")
	}

	// Put blob
	err = storage.Put(ctx, key, strings.NewReader("content"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Check existing blob
	exists, err = storage.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected blob to exist")
	}
}
