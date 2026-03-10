// Example custom-sharding demonstrates different ShardFunc strategies.
//
// The ShardFunc controls how blob keys are mapped to filesystem paths.
// Different strategies suit different access patterns:
//
//   - Default (two-level hash): Good general-purpose distribution
//   - Date-based: Groups blobs by creation date for easy archival
//   - Bucket-based: Organizes by key prefix (built-in as BucketShardFunc)
//   - Single-level: Simpler structure, fine for small blob counts
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/alexjoedt/blobfs"
)

// dateShardFunc organizes blobs by date with hash-based subdirectories.
// Resulting path: "2025/03/09/<sha256-hash>"
//
// Use this when you frequently query or archive by date range.
func dateShardFunc(key string) string {
	hash := sha256.Sum256([]byte(key))
	hexHash := hex.EncodeToString(hash[:])
	now := time.Now()
	return filepath.Join(now.Format("2006/01/02"), hexHash)
}

// flatShardFunc uses a single directory level with the full hash.
// Resulting path: "<sha256-hash>"
//
// Simple but may cause performance issues with many files (>10,000)
// in a single parent directory, depending on the filesystem.
func flatShardFunc(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

// threeLevelShardFunc uses three levels for extremely large collections.
// Resulting path: "ab/cd/ef/<sha256-hash>"
//
// Creates 256^3 = 16,777,216 possible leaf directories.
func threeLevelShardFunc(key string) string {
	hash := sha256.Sum256([]byte(key))
	hexHash := hex.EncodeToString(hash[:])
	return filepath.Join(hexHash[:2], hexHash[2:4], hexHash[4:6], hexHash)
}

func main() {
	ctx := context.Background()

	strategies := []struct {
		name string
		fn   blobfs.ShardFunc
	}{
		{"default (two-level hash)", blobfs.DefaultShardFunc},
		{"bucket-based", blobfs.BucketShardFunc},
		{"date-based", dateShardFunc},
		{"flat", flatShardFunc},
		{"three-level", threeLevelShardFunc},
	}

	// Show how each strategy maps a key to a storage path
	keys := []string{
		"users/avatar.png",
		"documents/report.pdf",
		"backup.tar.gz",
	}

	fmt.Println("=== Shard path mapping ===")
	for _, s := range strategies {
		fmt.Printf("\n  Strategy: %s\n", s.name)
		for _, key := range keys {
			fmt.Printf("    %-30s -> %s\n", key, s.fn(key))
		}
	}

	// Demonstrate using a custom shard function with actual storage
	fmt.Println("\n=== Date-based sharding in action ===")

	dir, err := os.MkdirTemp("", "blobfs-sharding-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage, err := blobfs.NewStorage(dir,
		blobfs.WithShardFunc(dateShardFunc),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Store some blobs - they'll be organized by today's date
	for _, key := range keys {
		content := fmt.Sprintf("content of %s", key)
		if err := storage.Put(ctx, key, bytes.NewReader([]byte(content))); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  stored: %s\n", key)
	}

	// Walk and show the stored blobs
	fmt.Println("\n  Stored blobs:")
	err = storage.Walk(ctx, "", func(key string, meta *blobfs.Meta, err error) error {
		if err != nil {
			return err
		}
		fmt.Printf("    %s (sha256: %s...)\n", key, meta.Sha256[:16])
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}