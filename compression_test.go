package blobfs

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func TestCompression_Gzip_RoundTrip(t *testing.T) {
	bs, err := NewStorage(t.TempDir(), WithCompression(CodecGzip))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	original := bytes.Repeat([]byte("hello world "), 1000)

	if err := bs.Put(ctx, "compressed/test.txt", bytes.NewReader(original)); err != nil {
		t.Fatal(err)
	}

	meta, err := bs.Stat(ctx, "compressed/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Compression != string(CodecGzip) {
		t.Errorf("expected compression %q, got %q", CodecGzip, meta.Compression)
	}
	if meta.StoredSize >= meta.Size {
		t.Errorf("expected stored size (%d) < original size (%d)", meta.StoredSize, meta.Size)
	}

	rc, err := bs.Get(ctx, "compressed/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Error("round-trip content mismatch")
	}
}

func TestCompression_MixedStorage(t *testing.T) {
	// Blobs written without compression must still be readable when
	// the storage is re-opened with WithCompression.
	dir := t.TempDir()

	plain, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := plain.Put(context.Background(), "legacy/blob.txt", strings.NewReader("uncompressed")); err != nil {
		t.Fatal(err)
	}

	compressed, err := NewStorage(dir, WithCompression(CodecGzip))
	if err != nil {
		t.Fatal(err)
	}
	rc, err := compressed.Get(context.Background(), "legacy/blob.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "uncompressed" {
		t.Errorf("expected %q, got %q", "uncompressed", string(got))
	}
}

func TestCompression_MetaFields(t *testing.T) {
	bs, err := NewStorage(t.TempDir(), WithCompression(CodecGzip))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	content := []byte("some content for testing metadata fields")

	if err := bs.Put(ctx, "meta/test.txt", bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	meta, err := bs.Stat(ctx, "meta/test.txt")
	if err != nil {
		t.Fatal(err)
	}

	if meta.Size != int64(len(content)) {
		t.Errorf("Size: expected %d, got %d", len(content), meta.Size)
	}
	if meta.StoredSize == 0 {
		t.Error("StoredSize should be non-zero for a compressed blob")
	}
	if meta.Compression != string(CodecGzip) {
		t.Errorf("Compression: expected %q, got %q", CodecGzip, meta.Compression)
	}
}

func TestCompression_NoCompression_StoredSizeSet(t *testing.T) {
	bs, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	content := []byte("plain uncompressed content")

	if err := bs.Put(ctx, "plain/test.txt", bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	meta, err := bs.Stat(ctx, "plain/test.txt")
	if err != nil {
		t.Fatal(err)
	}

	if meta.Compression != "" {
		t.Errorf("expected empty Compression, got %q", meta.Compression)
	}
	if meta.StoredSize != meta.Size {
		t.Errorf("expected StoredSize (%d) == Size (%d) for uncompressed blob", meta.StoredSize, meta.Size)
	}
}

func TestCompression_WithVerifyOnRead(t *testing.T) {
	bs, err := NewStorage(t.TempDir(), WithCompression(CodecGzip), WithVerifyOnRead(true))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	original := bytes.Repeat([]byte("verifiable compressed content "), 500)

	if err := bs.Put(ctx, "verify/compressed.txt", bytes.NewReader(original)); err != nil {
		t.Fatal(err)
	}

	rc, err := bs.Get(ctx, "verify/compressed.txt")
	if err != nil {
		t.Fatal(err)
	}

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Errorf("unexpected error on clean compressed blob: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Error("round-trip content mismatch with verification enabled")
	}
}

func TestCompression_Zstd_RoundTrip(t *testing.T) {
	bs, err := NewStorage(t.TempDir(), WithCompression(CodecZstd))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	original := bytes.Repeat([]byte("hello world "), 1000)

	if err := bs.Put(ctx, "compressed/test.txt", bytes.NewReader(original)); err != nil {
		t.Fatal(err)
	}

	meta, err := bs.Stat(ctx, "compressed/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Compression != string(CodecZstd) {
		t.Errorf("expected compression %q, got %q", CodecZstd, meta.Compression)
	}
	if meta.StoredSize >= meta.Size {
		t.Errorf("expected stored size (%d) < original size (%d)", meta.StoredSize, meta.Size)
	}

	rc, err := bs.Get(ctx, "compressed/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Error("zstd round-trip content mismatch")
	}
}

func TestCompression_Zstd_MixedStorage(t *testing.T) {
	// A blob written without compression must still be readable when the
	// storage is opened with zstd.
	dir := t.TempDir()

	plain, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := plain.Put(context.Background(), "legacy/blob.txt", strings.NewReader("uncompressed")); err != nil {
		t.Fatal(err)
	}

	zs, err := NewStorage(dir, WithCompression(CodecZstd))
	if err != nil {
		t.Fatal(err)
	}
	rc, err := zs.Get(context.Background(), "legacy/blob.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "uncompressed" {
		t.Errorf("expected %q, got %q", "uncompressed", string(got))
	}
}

func TestCompression_Zstd_WithVerifyOnRead(t *testing.T) {
	bs, err := NewStorage(t.TempDir(), WithCompression(CodecZstd), WithVerifyOnRead(true))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	original := bytes.Repeat([]byte("verifiable zstd content "), 500)

	if err := bs.Put(ctx, "verify/compressed.txt", bytes.NewReader(original)); err != nil {
		t.Fatal(err)
	}

	rc, err := bs.Get(ctx, "verify/compressed.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Errorf("unexpected error on clean zstd blob: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Error("round-trip content mismatch with zstd + verification")
	}
}


func TestCompression_Dedup_CompressedContent(t *testing.T) {
	// Two keys with identical content should deduplicate even when compressed.
	bs, err := NewStorage(t.TempDir(), WithCompression(CodecGzip))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	content := bytes.Repeat([]byte("deduplication test content "), 200)

	if err := bs.Put(ctx, "dedup/a", bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Put(ctx, "dedup/b", bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	metaA, err := bs.Stat(ctx, "dedup/a")
	if err != nil {
		t.Fatal(err)
	}
	metaB, err := bs.Stat(ctx, "dedup/b")
	if err != nil {
		t.Fatal(err)
	}
	if metaA.Sha256 != metaB.Sha256 {
		t.Error("expected same SHA-256 for identical content")
	}

	// Both keys must read back the original content.
	for _, key := range []string{"dedup/a", "dedup/b"} {
		rc, err := bs.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("ReadAll(%q): %v", key, err)
		}
		if !bytes.Equal(got, content) {
			t.Errorf("key %q: content mismatch after round-trip", key)
		}
	}
}
