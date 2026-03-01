package blobfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCreatePathFromKey(t *testing.T) {
	expected := "test/blobs/b3/91/b39131e703bbbf2cb97a2c1b1e03c27778003d2c4cfbda994b7be8a97f1df296"
	bs := &Storage{
		root: "./test",
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

	// Verify the blobs directory still exists
	blobsDir := filepath.Join(bs.root, "blobs")
	if _, err := os.Stat(blobsDir); os.IsNotExist(err) {
		t.Error("blobs directory should not be removed by cleanup")
	}

	// Verify root directory still exists
	if _, err := os.Stat(bs.root); os.IsNotExist(err) {
		t.Error("root directory should not be removed by cleanup")
	}
}

func TestList(t *testing.T) {
	// Clean up any existing test data first to ensure isolation
	os.RemoveAll("./test")

	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.RemoveAll("./test")
	})

	// Put some test blobs with different prefixes
	testData := map[string]string{
		"users/alice/avatar.jpg":     "alice avatar",
		"users/alice/profile.json":   "alice profile",
		"users/bob/avatar.jpg":       "bob avatar",
		"documents/2024/invoice.pdf": "invoice",
		"documents/2025/report.pdf":  "report",
		"temp/file.txt":              "temp file",
	}

	for key, content := range testData {
		err := bs.Put(t.Context(), key, strings.NewReader(content))
		if err != nil {
			t.Fatalf("failed to put %s: %v", key, err)
		}
	}

	tests := []struct {
		name     string
		prefix   string
		expected []string
	}{
		{
			name:   "list all",
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
			name:   "list users",
			prefix: "users/",
			expected: []string{
				"users/alice/avatar.jpg",
				"users/alice/profile.json",
				"users/bob/avatar.jpg",
			},
		},
		{
			name:   "list alice",
			prefix: "users/alice/",
			expected: []string{
				"users/alice/avatar.jpg",
				"users/alice/profile.json",
			},
		},
		{
			name:     "list documents 2024",
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
			iter := bs.List(t.Context(), tt.prefix)
			defer iter.Close()

			var found []string
			for iter.Next() {
				found = append(found, iter.Key())
			}

			if err := iter.Err(); err != nil {
				t.Fatalf("iteration error: %v", err)
			}

			// Check if all expected keys are found
			for _, expected := range tt.expected {
				if !contains(found, expected) {
					t.Errorf("expected key %q not found in results", expected)
				}
			}

			// Check if no unexpected keys are found
			for _, key := range found {
				if !contains(tt.expected, key) {
					t.Errorf("unexpected key %q in results", key)
				}
			}
		})
	}
}

func TestListEarlyExit(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	// Put multiple blobs
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("early-exit/file-%d.txt", i)
		err := bs.Put(t.Context(), key, strings.NewReader(fmt.Sprintf("content %d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	// List but stop after 3 items
	iter := bs.List(t.Context(), "early-exit/")
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
		if count >= 3 {
			break // Early exit
		}
	}

	if err := iter.Err(); err != nil {
		t.Fatalf("iteration error: %v", err)
	}

	if count != 3 {
		t.Errorf("expected to iterate 3 times, got %d", count)
	}
}

func TestListContextCancellation(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	// Put multiple blobs
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("cancel-test/file-%d.txt", i)
		err := bs.Put(t.Context(), key, strings.NewReader(fmt.Sprintf("content %d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Create cancellable context
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	iter := bs.List(ctx, "cancel-test/")
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
		if count >= 2 {
			cancel() // Cancel context
		}
	}

	// Should have stopped due to cancellation
	if err := iter.Err(); err == nil {
		t.Error("expected context cancellation error")
	}
}

func TestListMetadata(t *testing.T) {
	bs, err := NewStorage("./test")
	if err != nil {
		t.Fatal(err)
	}

	key := "meta-test/file.txt"
	content := "test content for metadata"
	err = bs.Put(t.Context(), key, strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	iter := bs.List(t.Context(), "meta-test/")
	defer iter.Close()

	if !iter.Next() {
		t.Fatal("expected at least one result")
	}

	meta := iter.Meta()
	if meta == nil {
		t.Fatal("expected metadata")
	}

	if meta.Key != key {
		t.Errorf("expected key %q, got %q", key, meta.Key)
	}

	if meta.Size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), meta.Size)
	}

	if meta.Sha256 == "" {
		t.Error("expected sha256 hash")
	}

	if meta.ContentType == "" {
		t.Error("expected content type")
	}

	if meta.CreatedAt.IsZero() {
		t.Error("expected non-zero created time")
	}

	if meta.ModifiedAt.IsZero() {
		t.Error("expected non-zero modified time")
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
			err := bs.Walk(t.Context(), tt.prefix, func(key string, meta *Meta, err error) error {
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
	err = bs.Walk(t.Context(), "walk-early-exit/", func(key string, meta *Meta, err error) error {
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
	err = bs.Walk(ctx, "walk-cancel-test/", func(key string, meta *Meta, err error) error {
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
	err = bs.Walk(t.Context(), "walk-meta-test/", func(k string, meta *Meta, err error) error {
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

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
