package blobfs

import (
	"context"
	"io"
	"strings"
	"testing"
)

// TestE2E_CodecSwitchLifecycle drives one store directory through repeated
// codec changes using only the public API. Every key must read back
// correctly from every store, and GC must reclaim exactly the objects that
// lost their last reference.
func TestE2E_CodecSwitchLifecycle(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	const content = "shared content across codecs"

	open := func(opts ...OptionFunc) *Storage {
		t.Helper()
		bs, err := NewStorage(dir, opts...)
		if err != nil {
			t.Fatal(err)
		}
		return bs
	}
	put := func(bs *Storage, key string) {
		t.Helper()
		if err := bs.Put(ctx, key, strings.NewReader(content)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	readAll := func(bs *Storage, key string) {
		t.Helper()
		rc, err := bs.Get(ctx, key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		if string(got) != content {
			t.Fatalf("%s: got %q", key, got)
		}
	}

	gc := func(bs *Storage, wantScanned, wantRemoved int) {
		t.Helper()
		stats, err := bs.GC(ctx)
		if err != nil {
			t.Fatalf("gc: %v", err)
		}
		if stats.ObjectsScanned != wantScanned || stats.ObjectsRemoved != wantRemoved {
			t.Fatalf("gc: scanned %d removed %d, want %d/%d",
				stats.ObjectsScanned, stats.ObjectsRemoved, wantScanned, wantRemoved)
		}
	}

	plain := open()
	put(plain, "a")
	put(plain, "b")

	gz := open(WithCompression(CodecGzip))
	put(gz, "a")

	zs := open(WithCompression(CodecZstd))
	put(zs, "c")

	plain2 := open()
	put(plain2, "b")

	stores := []*Storage{plain, gz, zs, plain2}
	for _, bs := range stores {
		for _, key := range []string{"a", "b", "c"} {
			readAll(bs, key)
		}
	}

	want := map[string]Codec{"a": CodecGzip, "b": CodecNone, "c": CodecZstd}
	for key, codec := range want {
		meta, err := plain.Stat(ctx, key)
		if err != nil {
			t.Fatalf("stat %s: %v", key, err)
		}
		if Codec(meta.Compression) != codec {
			t.Errorf("%s: compression %q, want %q", key, meta.Compression, codec)
		}
	}

	for _, key := range []string{"a", "c"} {
		if err := plain.Delete(ctx, key); err != nil {
			t.Fatalf("delete %s: %v", key, err)
		}
	}
	gc(plain, 3, 2)
	for _, bs := range stores {
		readAll(bs, "b")
	}

	if err := plain.Delete(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	gc(plain, 1, 1)
}
