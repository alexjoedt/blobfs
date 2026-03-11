package blobfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCreatePathFromKey(t *testing.T) {
	expected := "test/refs/b3/91/b39131e703bbbf2cb97a2c1b1e03c27778003d2c4cfbda994b7be8a97f1df296"
	bs := &Storage{
		root:    "./test",
		refsDir: "test/refs",
		opts: &Options{
			ShardFunc: DefaultShardFunc,
		},
	}
	h := bs.createPathFromKey("invoice/2025/customer.pdf")
	if h != expected {
		t.Errorf("expected %s, got %s", expected, h)
		t.FailNow()
	}
}

func TestPutUpdate(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	key := "update/test.txt"

	// First put
	r1 := strings.NewReader("initial content")
	err = bs.Put(t.Context(), key, r1)
	if err != nil {
		t.Fatal(err)
	}

	// Read metadata
	storagePath := bs.createPathFromKey(key)
	metaPath := storagePath + "/meta.json"
	meta1, err := bs.readMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}

	// Wait a bit to ensure different timestamps
	time.Sleep(10 * time.Millisecond)

	// Second put (update)
	r2 := strings.NewReader("updated content with different size")
	err = bs.Put(t.Context(), key, r2)
	if err != nil {
		t.Fatal(err)
	}

	// Read updated metadata
	meta2, err := bs.readMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}

	// Verify createdAt is preserved
	if !meta1.CreatedAt.Equal(meta2.CreatedAt) {
		t.Errorf("createdAt changed: original %v, updated %v", meta1.CreatedAt, meta2.CreatedAt)
	}

	// Verify modifiedAt is updated
	if !meta2.ModifiedAt.After(meta1.ModifiedAt) {
		t.Errorf("modifiedAt not updated: original %v, updated %v", meta1.ModifiedAt, meta2.ModifiedAt)
	}

	// Verify size is updated
	if meta2.Size == meta1.Size {
		t.Error("size not updated")
	}

	// Verify sha256 is updated
	if meta2.Sha256 == meta1.Sha256 {
		t.Error("sha256 not updated")
	}
}

func TestDeleteIdempotent(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	key := "delete/idempotent.txt"

	// Put a blob
	r := strings.NewReader("test content")
	err = bs.Put(t.Context(), key, r)
	if err != nil {
		t.Fatal(err)
	}

	// First delete - should succeed
	err = bs.Delete(t.Context(), key)
	if err != nil {
		t.Errorf("first delete failed: %v", err)
	}

	// Second delete - should also succeed (idempotent)
	err = bs.Delete(t.Context(), key)
	if err != nil {
		t.Errorf("second delete failed (should be idempotent): %v", err)
	}

	// Third delete - just to be sure
	err = bs.Delete(t.Context(), key)
	if err != nil {
		t.Errorf("third delete failed (should be idempotent): %v", err)
	}
}

func TestDeleteCleansUpEmptyDirs(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	key := "cleanup/nested/path/file.txt"

	// Put a blob
	r := strings.NewReader("test content")
	err = bs.Put(t.Context(), key, r)
	if err != nil {
		t.Fatal(err)
	}

	// Get the storage path to check directories
	storagePath := bs.createPathFromKey(key)
	parent1 := filepath.Dir(storagePath) // blobs/xx/yy
	parent2 := filepath.Dir(parent1)     // blobs/xx

	// Verify the directories exist before deletion
	if _, err := os.Stat(parent1); os.IsNotExist(err) {
		t.Fatal("parent directory should exist before deletion")
	}

	// Delete the blob
	err = bs.Delete(t.Context(), key)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Verify the blob storage path is gone
	if _, err := os.Stat(storagePath); !os.IsNotExist(err) {
		t.Error("blob storage path should be deleted")
	}

	// Verify parent directories are cleaned up if empty
	// Note: They might not be empty if other tests have created files there
	// So we just verify that cleanup was attempted (no error from delete)
	_, err1 := os.Stat(parent1)
	_, err2 := os.Stat(parent2)

	// If both still exist, they must not be empty (other blobs present)
	// If they don't exist, cleanup worked
	if err1 == nil {
		// Directory still exists, check if it's not empty
		entries, _ := os.ReadDir(parent1)
		if len(entries) == 0 {
			t.Error("empty parent directory was not cleaned up")
		}
	}

	t.Logf("Cleanup test completed - parent1 exists: %v, parent2 exists: %v", err1 == nil, err2 == nil)
}

func TestDeleteDoesNotRemoveBlobsDir(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	key := "safety/test.txt"

	// Put and delete a blob
	r := strings.NewReader("test content")
	err = bs.Put(t.Context(), key, r)
	if err != nil {
		t.Fatal(err)
	}

	err = bs.Delete(t.Context(), key)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Verify the refs directory still exists
	refsDir := filepath.Join(bs.root, refsDirName)
	if _, err := os.Stat(refsDir); os.IsNotExist(err) {
		t.Error("refs directory should not be removed by cleanup")
	}

	// Verify root directory still exists
	if _, err := os.Stat(bs.root); os.IsNotExist(err) {
		t.Error("root directory should not be removed by cleanup")
	}
}

func TestWalk(t *testing.T) {
	os.RemoveAll("./test")

	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.RemoveAll("./test")
	})

	testData := map[string]string{
		"users/alice/avatar.jpg":     "alice avatar",
		"users/alice/profile.json":   "alice profile",
		"users/bob/avatar.jpg":       "bob avatar",
		"documents/2024/invoice.pdf": "invoice",
		"documents/2025/report.pdf":  "report",
		"temp/file.txt":              "temp file",
	}

	for key, content := range testData {
		if err := bs.Put(t.Context(), key, strings.NewReader(content)); err != nil {
			t.Fatalf("failed to put %s: %v", key, err)
		}
	}

	tests := []struct {
		name     string
		prefix   string
		expected []string
	}{
		{
			name:   "walk all",
			prefix: "",
			expected: []string{
				"users/alice/avatar.jpg",
				"users/alice/profile.json",
				"users/bob/avatar.jpg",
				"documents/2024/invoice.pdf",
				"documents/2025/report.pdf",
				"temp/file.txt",
			},
		},
		{
			name:   "walk users",
			prefix: "users/",
			expected: []string{
				"users/alice/avatar.jpg",
				"users/alice/profile.json",
				"users/bob/avatar.jpg",
			},
		},
		{
			name:   "walk alice",
			prefix: "users/alice/",
			expected: []string{
				"users/alice/avatar.jpg",
				"users/alice/profile.json",
			},
		},
		{
			name:     "walk documents 2024",
			prefix:   "documents/2024/",
			expected: []string{"documents/2024/invoice.pdf"},
		},
		{
			name:     "no match",
			prefix:   "nonexistent/",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var found []string
			err := bs.Walk(t.Context(), tt.prefix, func(key string, _ *Meta, err error) error {
				if err != nil {
					return err
				}
				found = append(found, key)
				return nil
			})
			if err != nil {
				t.Fatalf("walk error: %v", err)
			}

			for _, expected := range tt.expected {
				if !contains(found, expected) {
					t.Errorf("expected key %q not found in results", expected)
				}
			}
			for _, key := range found {
				if !contains(tt.expected, key) {
					t.Errorf("unexpected key %q in results", key)
				}
			}
		})
	}
}

func TestWalkEarlyExit(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("walk-early-exit/file-%d.txt", i)
		if err := bs.Put(t.Context(), key, strings.NewReader(fmt.Sprintf("content %d", i))); err != nil {
			t.Fatal(err)
		}
	}

	count := 0
	err = bs.Walk(t.Context(), "walk-early-exit/", func(_ string, _ *Meta, err error) error {
		if err != nil {
			return err
		}
		count++
		if count >= 3 {
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected to visit 3 blobs, got %d", count)
	}
}

func TestWalkContextCancellation(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("walk-cancel-test/file-%d.txt", i)
		if err := bs.Put(t.Context(), key, strings.NewReader(fmt.Sprintf("content %d", i))); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	count := 0
	err = bs.Walk(ctx, "walk-cancel-test/", func(_ string, _ *Meta, err error) error {
		if err != nil {
			return err
		}
		count++
		if count >= 2 {
			cancel()
		}
		return nil
	})

	if err == nil {
		t.Error("expected context cancellation error")
	}
}

func TestWalkMetadata(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	key := "walk-meta-test/file.txt"
	content := "test content for walk metadata"
	if err := bs.Put(t.Context(), key, strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	var got *Meta
	err = bs.Walk(t.Context(), "walk-meta-test/", func(_ string, meta *Meta, err error) error {
		if err != nil {
			return err
		}
		got = meta
		return nil
	})
	if err != nil {
		t.Fatalf("walk error: %v", err)
	}
	if got == nil {
		t.Fatal("expected metadata, got nil")
	}
	if got.Key != key {
		t.Errorf("expected key %q, got %q", key, got.Key)
	}
	if got.Size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), got.Size)
	}
	if got.Sha256 == "" {
		t.Error("expected sha256 hash")
	}
	if got.ContentType == "" {
		t.Error("expected content type")
	}
	if got.CreatedAt.IsZero() {
		t.Error("expected non-zero created time")
	}
	if got.ModifiedAt.IsZero() {
		t.Error("expected non-zero modified time")
	}
}

func TestOpen(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	content := []byte("range read content for testing seeks")
	key := "seek/test.bin"

	// Put the content
	err = bs.Put(ctx, key, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	// Open the blob
	f, err := bs.Open(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Test seeking to offset 6
	offset, err := f.Seek(6, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 6 {
		t.Errorf("expected seek offset 6, got %d", offset)
	}

	// Read from offset 6
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	want := content[6:]
	if !bytes.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestOpenSeekEnd(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	content := []byte("test content for seek end")
	key := "seek/end-test.bin"

	// Put the content
	err = bs.Put(ctx, key, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	// Open the blob
	f, err := bs.Open(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Seek to 5 bytes before the end
	offset, err := f.Seek(-5, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	expectedOffset := int64(len(content) - 5)
	if offset != expectedOffset {
		t.Errorf("expected seek offset %d, got %d", expectedOffset, offset)
	}

	// Read remaining 5 bytes
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	want := content[len(content)-5:]
	if !bytes.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestOpenSeekCurrent(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	content := []byte("test content for seek current")
	key := "seek/current-test.bin"

	// Put the content
	err = bs.Put(ctx, key, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	// Open the blob
	f, err := bs.Open(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Read first 4 bytes
	buf := make([]byte, 4)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("expected to read 4 bytes, got %d", n)
	}

	// Seek 2 bytes forward from current position
	offset, err := f.Seek(2, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 6 {
		t.Errorf("expected seek offset 6 (after reading 4 and seeking 2), got %d", offset)
	}

	// Read remaining content
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	want := content[6:]
	if !bytes.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestOpenNotFound(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Try to open a non-existent blob
	_, err = bs.Open(ctx, "nonexistent/blob.txt")
	if err == nil {
		t.Error("expected error for non-existent blob")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestOpenInvalidKey(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Try to open with empty key
	_, err = bs.Open(ctx, "")
	if err == nil {
		t.Error("expected error for empty key")
	}
	if !errors.Is(err, ErrEmptyKey) {
		t.Errorf("expected ErrEmptyKey, got %v", err)
	}

	// Try to open with path traversal attempt
	_, err = bs.Open(ctx, "../../../etc/passwd")
	if err == nil {
		t.Error("expected error for path traversal attempt")
	}
	if !errors.Is(err, ErrInvalidKey) {
		t.Errorf("expected ErrInvalidKey, got %v", err)
	}
}

func TestVerifyOnRead_Clean(t *testing.T) {
	os.RemoveAll("./test")
	t.Cleanup(func() {
		os.RemoveAll("./test")
	})

	bs, err := NewStorage("./test", WithVerifyOnRead(true))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	key := "verify/clean.txt"
	content := "hello"

	// Put a blob
	err = bs.Put(ctx, key, strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	// Get the blob with verification enabled
	rc, err := bs.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	// Read all content
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}

	// Close should succeed (hash matches)
	if err := rc.Close(); err != nil {
		t.Errorf("unexpected error on clean blob: %v", err)
	}

	// Verify content
	if string(data) != content {
		t.Errorf("content mismatch: expected %q, got %q", content, string(data))
	}
}

func TestVerifyOnRead_Corrupted(t *testing.T) {
	os.RemoveAll("./test")
	t.Cleanup(func() {
		os.RemoveAll("./test")
	})

	bs, err := NewStorage("./test", WithVerifyOnRead(true))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	key := "verify/corrupt.txt"
	content := "hello"

	// Put a blob
	err = bs.Put(ctx, key, strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the data file directly
	storagePath := bs.createPathFromKey(key)
	dataPath := filepath.Join(storagePath, blobFileName)
	err = os.WriteFile(dataPath, []byte("XXXX!"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Get the blob with verification enabled
	rc, err := bs.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	// Read all content (this is corrupted data)
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}

	// Close should fail with ErrCorrupted
	err = rc.Close()
	if err == nil {
		t.Error("expected ErrCorrupted on corrupted blob")
	}
	if !errors.Is(err, ErrCorrupted) {
		t.Errorf("expected ErrCorrupted, got %v", err)
	}

	// Verify we got the corrupted data
	if string(data) != "XXXX!" {
		t.Errorf("unexpected data: expected 'XXXX!', got %q", string(data))
	}
}

func TestVerifyOnRead_Disabled(t *testing.T) {
	os.RemoveAll("./test")
	t.Cleanup(func() {
		os.RemoveAll("./test")
	})

	// Create storage WITHOUT verification
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	key := "verify/no-verify.txt"
	content := "hello"

	// Put a blob
	err = bs.Put(ctx, key, strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the data file directly
	storagePath := bs.createPathFromKey(key)
	dataPath := filepath.Join(storagePath, blobFileName)
	err = os.WriteFile(dataPath, []byte("XXXX!"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Get the blob WITHOUT verification enabled
	rc, err := bs.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	// Read all content
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}

	// Close should succeed even though blob is corrupted
	// (verification is disabled)
	if err := rc.Close(); err != nil {
		t.Errorf("unexpected error when verification is disabled: %v", err)
	}

	// Verify we got the corrupted data
	if string(data) != "XXXX!" {
		t.Errorf("unexpected data: expected 'XXXX!', got %q", string(data))
	}
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
