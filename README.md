# blobfs

A blob storage library for Go that stores files with keys on disk, with automatic deduplication and optional transparent compression.

> **Note**: This is a personal project for my own use. It works well for my needs, but it's not battle-tested for production environments.

## What it does

This library stores files (blobs) on disk using keys you provide. Internally it uses a content-addressable object store (`objects/`) backed by SHA-256 hashes, so identical content is stored only once regardless of the key used to store it. References (`refs/`) point to objects via hard links, keeping the inode count accurate for garbage collection.

You can use any key you want, like `"documents/invoice.pdf"` or `"user123/avatar.jpg"`. The key is hashed only to derive the directory structure — it does not need to be the content hash.

Key features:
- **Automatic deduplication** — blobs with identical content share a single on-disk object via hard links; no extra configuration needed.
- **Transparent compression** — store blobs compressed with gzip or Zstandard; reads decompress automatically.
- **Integrity verification** — optional SHA-256 hash check on every read.
- **Garbage collection** — reclaim space from orphaned objects after deletions.
- **Migration helper** — upgrade existing stores (v0.*.*) to the CAS layout.

## Installation

```bash
go get github.com/alexjoedt/blobfs
```

## Basic Usage

### Simple example

```go
package main

import (
    "context"
    "io"
    "log"
    "strings"

    "github.com/alexjoedt/blobfs"
)

func main() {
    ctx := context.Background()
    
    // Create storage in ./data directory
    storage, err := blobfs.NewStorage("./data")
    if err != nil {
        log.Fatal(err)
    }
    
    // Store a blob — identical content written under multiple keys is stored only once
    content := strings.NewReader("Hello, World!")
    err = storage.Put(ctx, "greetings/hello.txt", content)
    if err != nil {
        log.Fatal(err)
    }
    
    // Retrieve a blob
    reader, err := storage.Get(ctx, "greetings/hello.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer reader.Close()
    
    // Read the content
    data, _ := io.ReadAll(reader)
    println(string(data)) // Output: Hello, World!
}
```

### Example with compression and integrity verification

```go
package main

import (
    "context"
    "log"
    "strings"

    "github.com/alexjoedt/blobfs"
)

func main() {
    ctx := context.Background()
    
    // Enable Zstandard compression and read-time hash verification
    storage, err := blobfs.NewStorage("./data",
        blobfs.WithCompression(blobfs.CodecZstd),
        blobfs.WithVerifyOnRead(true),
        blobfs.WithFileMode(0600),
        blobfs.WithDirMode(0700),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    // Blobs are stored compressed; Get decompresses transparently
    blobs := map[string]string{
        "users/alice/profile.json": `{"name": "Alice"}`,
        "users/bob/profile.json":   `{"name": "Bob"}`,
        "docs/readme.md":           "# Documentation",
    }
    
    for key, content := range blobs {
        err := storage.Put(ctx, key, strings.NewReader(content))
        if err != nil {
            log.Printf("Failed to store %s: %v", key, err)
        }
    }
    
    // Walk all blobs with "users/" prefix
    err = storage.Walk(ctx, "users/", func(key string, meta *blobfs.Meta, err error) error {
        if err != nil {
            return err
        }
        println("Found:", meta.Key, "size:", meta.Size, "stored:", meta.StoredSize)
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

### Walk all blobs (no prefix filter)

```go
err := storage.Walk(ctx, "", func(key string, meta *blobfs.Meta, err error) error {
    if err != nil {
        return err // or return nil to skip corrupted entries
    }
    fmt.Println(key, meta.Size)
    return nil
})
```

Return `filepath.SkipAll` from the callback to stop iteration early without an error.

## API

| Method | Description |
|--------|-------------|
| `Put(ctx, key, reader)` | Store a blob |
| `Get(ctx, key)` | Retrieve a blob as `io.ReadCloser` (decompresses transparently) |
| `Open(ctx, key)` | Retrieve a blob as `io.ReadSeekCloser` (raw, no decompression; useful for `http.ServeContent`) |
| `Delete(ctx, key)` | Delete a blob |
| `Stat(ctx, key)` | Read metadata without fetching content |
| `Exists(ctx, key)` | Check whether a blob exists |
| `Walk(ctx, prefix, fn)` | Iterate blobs matching a prefix via callback |
| `GC(ctx)` | Remove orphaned objects from the object store; returns `GCStats` |
| `Migrate(ctx)` | Upgrade an existing store (v0.*.*) to the CAS layout; returns `MigrateStats` |

> **Deprecated:** `List` is deprecated in favour of `Walk` and will be removed in a future version.

## Deduplication

Deduplication is enabled by default. Internally the library maintains two directories:

- **`objects/`** — content-addressable store keyed by SHA-256. Each unique blob body is stored exactly once.
- **`refs/`** — one entry per user key. Each ref's data file is a hard link to the corresponding object.

When two keys refer to the same content, both refs point to the same inode; no bytes are duplicated on disk.

To upgrade a store (from v0.*.*) that was created before this layout was introduced, call `Migrate`:

```go
stats, err := storage.Migrate(ctx)
fmt.Printf("linked %d blobs, saved %d bytes\n", stats.BlobsLinked, stats.BytesSaved)
```

To reclaim space from objects that are no longer referenced by any key, call `GC` after deleting blobs:

```go
stats, err := storage.GC(ctx)
fmt.Printf("removed %d objects, reclaimed %d bytes\n", stats.ObjectsRemoved, stats.BytesReclaimed)
```

## Available Options

- **`WithFileMode(mode)`** - Set file permissions (default: 0644)
- **`WithDirMode(mode)`** - Set directory permissions (default: 0755)
- **`WithShardFunc(fn)`** - Set custom sharding strategy (see below)
- **`WithCompression(codec)`** - Enable transparent compression: `CodecGzip` or `CodecZstd` (default: `CodecNone`)
- **`WithVerifyOnRead(bool)`** - Verify SHA-256 hash on every read; `Close` returns `ErrCorrupted` on mismatch (default: false)

## Compression

Compression is opt-in. Set a codec when creating the storage and all subsequent writes will be stored compressed. Reads always decompress transparently regardless of whether the blob was written with compression.

```go
storage, err := blobfs.NewStorage("./data",
    blobfs.WithCompression(blobfs.CodecZstd), // or blobfs.CodecGzip
)
```

Each blob's metadata records which codec was used (`meta.json`), so old blobs written without compression remain readable after enabling compression — and vice versa. Changing or removing `WithCompression` never breaks existing data.

Available codecs:

| Constant | Description |
|----------|-------------|
| `CodecNone` | No compression (default) |
| `CodecGzip` | gzip — wide compatibility |
| `CodecZstd` | Zstandard — better ratio and speed than gzip |

`Meta.StoredSize` reports the compressed on-disk size; `Meta.Size` is always the original uncompressed size.

## Sharding Strategies

The library includes several sharding functions to organize your files:

- **`DefaultShardFunc`** - Two-level hash-based sharding (e.g., `bb/4d/bb4de5c4...`)
- **`BucketShardFunc`** - Extracts bucket from key (e.g., `users/alice/...` → `users/hash...`)

You can also write your own `ShardFunc` to customize how files are organized.

## License

MIT
