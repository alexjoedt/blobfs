package blobfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestBlob_Write(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("basic write", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		data := []byte("Hello, World!")
		n, err := blob.Write(data)
		if err != nil {
			t.Fatal(err)
		}

		if n != len(data) {
			t.Errorf("expected to write %d bytes, wrote %d", len(data), n)
		}

		if blob.Size() != int64(len(data)) {
			t.Errorf("expected size %d, got %d", len(data), blob.Size())
		}

		if err := blob.CommitAs("test/file.txt"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("multiple writes", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		writes := []string{"Hello", ", ", "World", "!"}
		totalSize := 0

		for _, s := range writes {
			n, err := blob.Write([]byte(s))
			if err != nil {
				t.Fatal(err)
			}
			totalSize += n
		}

		if blob.Size() != int64(totalSize) {
			t.Errorf("expected size %d, got %d", totalSize, blob.Size())
		}

		if err := blob.CommitAs("test/multiwrite.txt"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBlob_IoCopy(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("io.Copy from reader", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		content := "This is test content for io.Copy"
		reader := strings.NewReader(content)

		n, err := io.Copy(blob, reader)
		if err != nil {
			t.Fatal(err)
		}

		if n != int64(len(content)) {
			t.Errorf("expected to copy %d bytes, copied %d", len(content), n)
		}

		if blob.Size() != int64(len(content)) {
			t.Errorf("expected size %d, got %d", len(content), blob.Size())
		}

		if err := blob.CommitAs("test/copy.txt"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("io.Copy large file", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		// Generate 1MB of data
		size := 1024 * 1024
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		reader := bytes.NewReader(data)
		n, err := io.Copy(blob, reader)
		if err != nil {
			t.Fatal(err)
		}

		if n != int64(size) {
			t.Errorf("expected to copy %d bytes, copied %d", size, n)
		}

		if err := blob.CommitAs("test/large.bin"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBlob_CommitAs(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("commit creates blob", func(t *testing.T) {
		key := "test/closeable.txt"
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		content := "Test content"
		blob.Write([]byte(content))

		err = blob.CommitAs(key)
		if err != nil {
			t.Fatal(err)
		}

		// Verify blob was created
		ctx := t.Context()
		exists, err := storage.Exists(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Error("blob should exist after CommitAs()")
		}

		// Verify content
		meta, err := storage.Stat(ctx, key)
		if err != nil {
			t.Fatal(err)
		}

		if meta.Size != int64(len(content)) {
			t.Errorf("expected size %d, got %d", len(content), meta.Size)
		}

		if meta.Key != key {
			t.Errorf("expected key %q, got %q", key, meta.Key)
		}
	})

	t.Run("discard is idempotent", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}

		blob.Write([]byte("Test"))

		err1 := blob.Discard()
		err2 := blob.Discard()
		err3 := blob.Discard()

		if err1 != nil {
			t.Errorf("first discard failed: %v", err1)
		}

		if err2 != nil {
			t.Errorf("second discard should not fail: %v", err2)
		}

		if err3 != nil {
			t.Errorf("third discard should not fail: %v", err3)
		}
	})

	t.Run("write after close fails", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}

		blob.Write([]byte("Initial"))
		blob.Close()

		_, err = blob.Write([]byte("After close"))
		if err != ErrBlobClosed {
			t.Errorf("expected ErrBlobClosed, got %v", err)
		}
	})

	t.Run("commit with empty key fails", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		blob.Write([]byte("Test"))

		err = blob.CommitAs("")
		if err != ErrEmptyKey {
			t.Errorf("expected ErrEmptyKey, got %v", err)
		}
	})

	t.Run("close discards without committing", func(t *testing.T) {
		key := "test/should-not-exist.txt"
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}

		blob.Write([]byte("This should not be committed"))
		blob.Close() // Just discards

		ctx := t.Context()
		exists, _ := storage.Exists(ctx, key)
		if exists {
			t.Error("blob should not exist after Close() without CommitAs()")
		}
	})
}

func TestBlob_ContentType(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		content     []byte
		wantType    string
		checkPrefix bool
	}{
		{
			name:        "text file",
			content:     []byte("Hello, World!"),
			wantType:    "text/plain",
			checkPrefix: true,
		},
		{
			name:        "json file",
			content:     []byte(`{"key": "value"}`),
			wantType:    "text/plain",
			checkPrefix: true,
		},
		{
			name:     "binary file",
			content:  []byte{0xFF, 0xD8, 0xFF, 0xE0}, // JPEG header
			wantType: "image/jpeg",
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := fmt.Sprintf("test/content-%d.bin", i)
			blob, err := storage.NewBlob()
			if err != nil {
				t.Fatal(err)
			}
			defer blob.Discard()

			blob.Write(tt.content)
			if err := blob.CommitAs(key); err != nil {
				t.Fatal(err)
			}

			ctx := t.Context()
			meta, err := storage.Stat(ctx, key)
			if err != nil {
				t.Fatal(err)
			}

			if tt.checkPrefix {
				if !strings.HasPrefix(meta.ContentType, tt.wantType) {
					t.Errorf("expected content type to start with %q, got %q", tt.wantType, meta.ContentType)
				}
			} else {
				if meta.ContentType != tt.wantType {
					t.Errorf("expected content type %q, got %q", tt.wantType, meta.ContentType)
				}
			}
		})
	}
}

func TestBlob_PreserveCreatedAt(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	key := "test/preserve-created.txt"

	// Create first version
	blob1, err := storage.NewBlob()
	if err != nil {
		t.Fatal(err)
	}
	defer blob1.Discard()
	blob1.Write([]byte("Version 1"))
	blob1.CommitAs(key)

	meta1, err := storage.Stat(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	// Create second version (overwrite)
	blob2, err := storage.NewBlob()
	if err != nil {
		t.Fatal(err)
	}
	defer blob2.Discard()
	blob2.Write([]byte("Version 2 - Updated"))
	blob2.CommitAs(key)

	meta2, err := storage.Stat(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	// CreatedAt should be preserved
	if !meta1.CreatedAt.Equal(meta2.CreatedAt) {
		t.Errorf("CreatedAt not preserved: original=%v, updated=%v", meta1.CreatedAt, meta2.CreatedAt)
	}

	// ModifiedAt should be updated
	if !meta2.ModifiedAt.After(meta1.ModifiedAt) {
		t.Errorf("ModifiedAt should be updated: original=%v, updated=%v", meta1.ModifiedAt, meta2.ModifiedAt)
	}
}

func TestBlob_ReadAfterWrite(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	key := "test/readwrite.txt"
	content := "This is test content that we will read back"

	// Write using Blob
	blob, err := storage.NewBlob()
	if err != nil {
		t.Fatal(err)
	}
	defer blob.Discard()
	_, err = blob.Write([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	err = blob.CommitAs(key)
	if err != nil {
		t.Fatal(err)
	}

	// Read back using Get
	reader, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	readContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	if string(readContent) != content {
		t.Errorf("content mismatch:\nwrote: %q\nread:  %q", content, string(readContent))
	}
}

func TestBlob_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	numBlobs := 10

	// Create multiple blobs concurrently
	errChan := make(chan error, numBlobs)

	for i := 0; i < numBlobs; i++ {
		go func(idx int) {
			key := fmt.Sprintf("test/concurrent-%d.txt", idx)
			blob, err := storage.NewBlob()
			if err != nil {
				errChan <- err
				return
			}
			defer blob.Discard()

			content := fmt.Sprintf("Content for blob %d", idx)
			_, err = io.Copy(blob, strings.NewReader(content))
			if err != nil {
				errChan <- err
				return
			}

			err = blob.CommitAs(key)
			errChan <- err
		}(i)
	}

	// Check all operations succeeded
	for i := 0; i < numBlobs; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("blob %d failed: %v", i, err)
		}
	}

	// Verify all blobs exist
	for i := 0; i < numBlobs; i++ {
		key := fmt.Sprintf("test/concurrent-%d.txt", i)
		exists, err := storage.Exists(ctx, key)
		if err != nil {
			t.Errorf("checking blob %d: %v", i, err)
		}
		if !exists {
			t.Errorf("blob %d should exist", i)
		}
	}
}

func TestBlob_FmtFprintf(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	key := "test/formatted.txt"

	blob, err := storage.NewBlob()
	if err != nil {
		t.Fatal(err)
	}
	defer blob.Discard()

	// Use fmt.Fprintf to write formatted content
	fmt.Fprintf(blob, "Line 1: %s\n", "Hello")
	fmt.Fprintf(blob, "Line 2: %d\n", 42)
	fmt.Fprintf(blob, "Line 3: %v\n", true)

	if err := blob.CommitAs(key); err != nil {
		t.Fatal(err)
	}

	// Read back and verify
	reader, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	expected := "Line 1: Hello\nLine 2: 42\nLine 3: true\n"
	if string(content) != expected {
		t.Errorf("content mismatch:\nexpected: %q\ngot:      %q", expected, string(content))
	}
}

func TestBlob_Hash(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("hash matches content", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		content := []byte("Test content for hashing")
		if _, err := blob.Write(content); err != nil {
			t.Fatal(err)
		}

		// Compute expected hash
		expectedHash := sha256.Sum256(content)
		expectedHashStr := hex.EncodeToString(expectedHash[:])

		// Get hash from blob
		gotHash := blob.Hash()

		if gotHash != expectedHashStr {
			t.Errorf("hash mismatch:\nexpected: %s\ngot:      %s", expectedHashStr, gotHash)
		}
	})

	t.Run("hash available before commit", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		if _, err := blob.Write([]byte("content")); err != nil {
			t.Fatal(err)
		}

		// Hash should be available without committing
		hash := blob.Hash()
		if hash == "" {
			t.Error("hash should not be empty before commit")
		}

		if len(hash) != 64 { // SHA-256 hex length
			t.Errorf("expected hash length 64, got %d", len(hash))
		}
	})

	t.Run("empty blob hash", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		// Hash of empty content
		hash := blob.Hash()

		// SHA-256 of empty string
		emptyHash := sha256.Sum256([]byte{})
		expectedHash := hex.EncodeToString(emptyHash[:])

		if hash != expectedHash {
			t.Errorf("empty hash mismatch:\nexpected: %s\ngot:      %s", expectedHash, hash)
		}
	})

	t.Run("hash updates with writes", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		// First write
		blob.Write([]byte("Hello"))
		hash1 := blob.Hash()

		// Second write
		blob.Write([]byte(", World!"))
		hash2 := blob.Hash()

		// Hashes should be different
		if hash1 == hash2 {
			t.Error("hash should change after additional writes")
		}

		// Final hash should match complete content
		completeContent := []byte("Hello, World!")
		expectedHash := sha256.Sum256(completeContent)
		expectedHashStr := hex.EncodeToString(expectedHash[:])

		if hash2 != expectedHashStr {
			t.Errorf("final hash mismatch:\nexpected: %s\ngot:      %s", expectedHashStr, hash2)
		}
	})
}

func TestBlob_Meta(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("meta is nil before commit", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		if blob.Meta() != nil {
			t.Error("expected Meta() to return nil before commit")
		}
	})

	t.Run("meta available after commit", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		data := []byte("Test content for metadata")
		expectedHash := sha256.Sum256(data)
		expectedHashStr := hex.EncodeToString(expectedHash[:])

		if _, err := blob.Write(data); err != nil {
			t.Fatal(err)
		}

		key := "test/metadata.txt"
		if err := blob.CommitAs(key); err != nil {
			t.Fatal(err)
		}

		meta := blob.Meta()
		if meta == nil {
			t.Fatal("expected Meta() to return metadata after commit")
		}

		if meta.Key != key {
			t.Errorf("expected key %q, got %q", key, meta.Key)
		}

		if meta.Size != int64(len(data)) {
			t.Errorf("expected size %d, got %d", len(data), meta.Size)
		}

		if meta.Sha256 != expectedHashStr {
			t.Errorf("expected hash %s, got %s", expectedHashStr, meta.Sha256)
		}

		if meta.ContentType != "text/plain; charset=utf-8" {
			t.Errorf("expected content type %q, got %q", "text/plain; charset=utf-8", meta.ContentType)
		}

		if meta.CreatedAt.IsZero() {
			t.Error("expected CreatedAt to be set")
		}

		if meta.ModifiedAt.IsZero() {
			t.Error("expected ModifiedAt to be set")
		}
	})

	t.Run("meta matches file metadata", func(t *testing.T) {
		blob, err := storage.NewBlob()
		if err != nil {
			t.Fatal(err)
		}
		defer blob.Discard()

		data := []byte(`{"test": "json"}`)
		if _, err := blob.Write(data); err != nil {
			t.Fatal(err)
		}

		key := "test/compare.json"
		if err := blob.CommitAs(key); err != nil {
			t.Fatal(err)
		}

		// Get metadata from blob
		blobMeta := blob.Meta()
		if blobMeta == nil {
			t.Fatal("expected Meta() to return metadata")
		}

		// Get metadata from storage (reads from file)
		ctx := t.Context()
		fileMeta, err := storage.Stat(ctx, key)
		if err != nil {
			t.Fatal(err)
		}

		// Compare
		if blobMeta.Key != fileMeta.Key {
			t.Errorf("key mismatch: blob=%q file=%q", blobMeta.Key, fileMeta.Key)
		}

		if blobMeta.Size != fileMeta.Size {
			t.Errorf("size mismatch: blob=%d file=%d", blobMeta.Size, fileMeta.Size)
		}

		if blobMeta.Sha256 != fileMeta.Sha256 {
			t.Errorf("hash mismatch: blob=%s file=%s", blobMeta.Sha256, fileMeta.Sha256)
		}

		if blobMeta.ContentType != fileMeta.ContentType {
			t.Errorf("content type mismatch: blob=%q file=%q", blobMeta.ContentType, fileMeta.ContentType)
		}
	})
}
