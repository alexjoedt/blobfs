// Example cas demonstrates content-addressable storage (CAS) using blobfs.
//
// In a CAS, the storage key is derived from the content itself (its SHA-256 hash).
// This provides automatic deduplication: storing the same content twice results
// in only one copy on disk.
//
// The key idea is using a custom ShardFunc that expects the key to already be
// a hex-encoded SHA-256 hash, and shards based on its prefix. Combined with
// the Blob API (NewBlob + Hash + CommitAs), this gives a clean CAS workflow.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/alexjoedt/blobfs"
)

// casShardFunc is a ShardFunc for content-addressable storage.
// It expects the key to be a hex-encoded SHA-256 hash and creates
// a two-level directory structure from its prefix: "ab/cd/<full-hash>"
func casShardFunc(key string) string {
	// The key IS the content hash in a CAS
	s1, s2 := key[:2], key[2:4]
	return filepath.Join(s1, s2, key)
}

func main() {
	dir, err := os.MkdirTemp("", "blobfs-cas-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Configure storage with our CAS shard function and a custom blob directory
	storage, err := blobfs.NewStorage(dir,
		blobfs.WithShardFunc(casShardFunc),
		blobfs.WithBlobDir("objects"),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// --- Approach 1: Using the Blob API for CAS writes ---
	// This is the recommended approach. NewBlob computes the hash while
	// writing, so you get the content hash without reading the data twice.

	fmt.Println("=== Blob API (recommended for CAS) ===")

	content1 := []byte("The quick brown fox jumps over the lazy dog")

	blob, err := storage.NewBlob()
	if err != nil {
		log.Fatal(err)
	}
	defer blob.Discard()

	if _, err := io.Copy(blob, bytes.NewReader(content1)); err != nil {
		log.Fatal(err)
	}

	// The hash is available before committing
	hash1 := blob.Hash()
	fmt.Printf("content hash: %s\n", hash1)

	// Check for deduplication: skip if already stored
	if exists, _ := storage.Exists(ctx, hash1); exists {
		fmt.Println("blob already exists, skipping")
	} else {
		if err := blob.CommitAs(hash1); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("stored as: %s\n", hash1)
	}

	// --- Approach 2: Pre-computed hash with Put ---
	// When you already know the hash (e.g., from an HTTP header or database),
	// you can use Put directly with the hash as key.

	fmt.Println("\n=== Pre-computed hash with Put ===")

	content2 := []byte("Another piece of content")
	hash2 := sha256Hex(content2)

	if err := storage.Put(ctx, hash2, bytes.NewReader(content2)); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("stored as: %s\n", hash2)

	// --- Demonstrate deduplication ---
	fmt.Println("\n=== Deduplication ===")

	// Store the same content again via the Blob API
	blob2, err := storage.NewBlob()
	if err != nil {
		log.Fatal(err)
	}
	defer blob2.Discard()

	if _, err := io.Copy(blob2, bytes.NewReader(content1)); err != nil {
		log.Fatal(err)
	}

	duplicateHash := blob2.Hash()
	fmt.Printf("duplicate hash: %s\n", duplicateHash)
	fmt.Printf("hashes match:  %v\n", hash1 == duplicateHash)

	if exists, _ := storage.Exists(ctx, duplicateHash); exists {
		fmt.Println("already exists - deduplication works!")
	}

	// --- Verify integrity on read ---
	fmt.Println("\n=== Integrity verification ===")

	rc, err := storage.Get(ctx, hash1)
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		log.Fatal(err)
	}

	// Recompute hash and compare
	verifyHash := sha256Hex(data)
	fmt.Printf("stored hash:   %s\n", hash1)
	fmt.Printf("verified hash: %s\n", verifyHash)
	fmt.Printf("integrity ok:  %v\n", hash1 == verifyHash)
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}