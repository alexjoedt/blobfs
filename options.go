package blobfs

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
)

// ShardFunc is a function that generates a storage path from a key.
// It receives the key and returns the relative path where the blob should be stored
// (without the filename). The returned path will be joined with the root directory.
//
// The function should:
//   - Return a path relative to root (e.g., "blobs/a3/f2/a3f29d4e8c...")
//   - Create a deterministic path based on the key
//   - Distribute keys evenly to avoid filesystem hotspots
//   - Be safe from path traversal attacks
//
// Example: For key "users/avatar.jpg", might return "blobs/a3/f2/a3f29d4e8c..."
type ShardFunc func(key string) string

// Options configures BlobStorage behavior.
type Options struct {
	FileMode  os.FileMode // Permission bits for blob data files
	DirMode   os.FileMode // Permission bits for directories
	ShardFunc ShardFunc   // Function to generate storage paths from keys
}

// OptionFunc is a functional option for configuring BlobStorage.
type OptionFunc func(opts *Options)

// WithFileMode sets the file permission mode for blob data files.
// Default is 0644 (owner read/write, group and others read-only).
func WithFileMode(mode os.FileMode) OptionFunc {
	return func(opts *Options) {
		opts.FileMode = mode
	}
}

// WithDirMode sets the directory permission mode for storage directories.
// Default is 0755 (owner read/write/execute, group and others read/execute).
func WithDirMode(mode os.FileMode) OptionFunc {
	return func(opts *Options) {
		opts.DirMode = mode
	}
}

// WithShardFunc sets a custom sharding function for generating storage paths.
// The function receives a key and returns a relative path (without filename).
//
// Default sharding: Two-level SHA-256 hash (e.g., "blobs/a3/f2/a3f29d4e8c...")
//
// Example custom sharding by date:
//
//	WithShardFunc(func(key string) string {
//	    now := time.Now()
//	    hash := sha256.Sum256([]byte(key))
//	    hexHash := hex.EncodeToString(hash[:])
//	    return filepath.Join("blobs", now.Format("2006/01/02"), hexHash)
//	})
//
// Example flat sharding (single directory per hash):
//
//	WithShardFunc(func(key string) string {
//	    hash := sha256.Sum256([]byte(key))
//	    hexHash := hex.EncodeToString(hash[:])
//	    return filepath.Join("blobs", hexHash)
//	})
func WithShardFunc(fn ShardFunc) OptionFunc {
	return func(opts *Options) {
		opts.ShardFunc = fn
	}
}

// DefaultShardFunc is the default two-level sharding strategy.
// Uses first 2 bytes of SHA-256 hash for two-level directory structure:
// "a3/f2/a3f29d4e8c..."
//
// Why two levels: Creates 256 * 256 = 65,536 possible directories,
// preventing filesystem performance degradation with large blob counts.
func DefaultShardFunc(key string) string {
	hash := sha256.Sum256([]byte(key))
	hexHash := hex.EncodeToString(hash[:])

	s1, s2 := hexHash[:2], hexHash[2:4]

	return filepath.Join(s1, s2, hexHash)
}

// BucketShardFunc organizes blobs into buckets based on key prefix.
// Extracts the first path segment as a bucket name, then applies
// two-level hash sharding within that bucket: "bucket/a3/f2/a3f29d4e8c..."
//
// Keys without "/" are placed in "misc" bucket.
// Example: "users/avatar.jpg" → "users/a3/f2/a3f29d4e8c..."
//
//	"document.pdf"    → "misc/b7/e4/b7e4c2a1f8..."
func BucketShardFunc(key string) string {
	hash := sha256.Sum256([]byte(key))
	hexHash := hex.EncodeToString(hash[:])

	s1, s2 := hexHash[:2], hexHash[2:4]

	// Extract first directory from key as bucket
	parts := strings.Split(key, "/")
	bucket := "misc"
	if len(parts) > 1 {
		bucket = parts[0]
	}
	return filepath.Join(bucket, s1, s2, hexHash)
}

// defaultOpts provides sensible default options.
// Why these defaults: 0644 for files allows owner to modify, others to read.
// 0755 for directories allows traversal while protecting against modification.
var defaultOpts = &Options{
	FileMode:  0644,
	DirMode:   0755,
	ShardFunc: DefaultShardFunc,
}
