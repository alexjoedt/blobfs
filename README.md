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
    "os"
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
    
    // List all blobs with "users/" prefix
    iter := storage.List(ctx, "users/")
    defer iter.Close()
    
    for iter.Next() {
        meta := iter.Meta()
        println("Found:", meta.Key, "Size:", meta.Size, "bytes")
    }
    
    if err := iter.Err(); err != nil {
        log.Fatal(err)
    }
}
```

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
