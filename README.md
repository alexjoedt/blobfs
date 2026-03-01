# blobfs

A simple blob storage library for Go that stores files with keys on disk.

> **Note**: This is a personal project for my own use. It works well for my needs, but it's not battle-tested for production environments.

## What it does

This library stores files (blobs) on disk using keys you provide. The keys are hashed to create a directory structure that keeps your filesystem organized, even with thousands of files.

**Important**: This is **not** a content-addressable storage (CAS). Your keys are not based on file content. Instead, the keys you provide are hashed only for creating an organized directory structure (sharding). You can use any key you want, like "documents/invoice.pdf" or "user123/avatar.jpg".

However, this library can be used as a foundation to build:
- A content-addressable storage (by using content hashes as keys)
- Any other key-value blob storage you need

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
    
    // Store a blob
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

### Example with options

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
    
    // Create storage with custom options
    storage, err := blobfs.NewStorage("./data",
        blobfs.WithFileMode(0600),              // Only owner can read/write
        blobfs.WithDirMode(0700),               // Only owner can access directories
        blobfs.WithShardFunc(blobfs.BucketShardFunc), // Use bucket-style sharding
    )
    if err != nil {
        log.Fatal(err)
    }
    
    // Store multiple blobs
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
        println("Found:", meta.Key, "Size:", meta.Size, "bytes")
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
| `Get(ctx, key)` | Retrieve a blob as `io.ReadCloser` |
| `Delete(ctx, key)` | Delete a blob |
| `Stat(ctx, key)` | Read metadata without fetching content |
| `Exists(ctx, key)` | Check whether a blob exists |
| `Walk(ctx, prefix, fn)` | Iterate blobs matching a prefix via callback |

> **Deprecated:** `List` is deprecated in favour of `Walk` and will be removed in a future version.

## Available Options

- **`WithFileMode(mode)`** - Set file permissions (default: 0644)
- **`WithDirMode(mode)`** - Set directory permissions (default: 0755)
- **`WithShardFunc(fn)`** - Set custom sharding strategy (see below)

## Sharding Strategies

The library includes several sharding functions to organize your files:

- **`DefaultShardFunc`** - Two-level hash-based sharding (e.g., `bb/4d/bb4de5c4...`)
- **`BucketShardFunc`** - Extracts bucket from key (e.g., `users/alice/...` → `users/hash...`)

You can also write your own `ShardFunc` to customize how files are organized.

## License

MIT
