// Package blobfs provides a content-addressable blob storage system
// that stores files using SHA-256 hashing for deduplication and integrity.
//
// # Design
//
// The storage uses a two-level directory structure based on the SHA-256 hash
// of the blob key, which prevents filesystem limitations with too many files
// in a single directory while maintaining fast lookups.
//
// Why content-addressable: Keys are hashed to create storage paths, which
// provides:
//   - Uniform distribution of files across directories
//   - Protection against path traversal attacks
//   - Predictable storage locations
//
// Why separate metadata: Metadata is stored in a separate JSON file to allow
// atomic updates and queries without reading the blob content.
//
// # Usage
//
// Basic operations:
//
//	storage, err := NewBlobStorage("/data/blobs")
//	if err != nil {
//		return err
//	}
//
//	// Store a blob
//	err = storage.Put(ctx, "documents/invoice.pdf", reader)
//
//	// Retrieve a blob
//	rc, err := storage.Get(ctx, "documents/invoice.pdf")
//	defer rc.Close()
//
//	// List blobs with prefix
//	iter := storage.List(ctx, "documents/")
//	defer iter.Close()
//	for iter.Next() {
//		meta := iter.Meta()
//		// process metadata...
//	}
//
// # Concurrency
//
// All operations are safe for concurrent use. Multiple goroutines may
// call methods on the same BlobStorage instance simultaneously.
//
// # Error Handling
//
// All methods return errors that can be unwrapped using errors.Is and
// errors.As for standard error types like os.ErrNotExist.
package blobfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	tempDirName  = ".tmp"
	blobDirname  = "blobs"
	metaFileName = "meta.json"
	blobFileName = "data"

	maxKeyLength = 1024
)

var (
	ErrKeyLengthExceeds = errors.New("maximal key length exceeds")
	ErrNotFound         = errors.New("blob with key not found")
	ErrEmptyKey         = errors.New("key cannot be empty")
	ErrInvalidKey       = errors.New("key contains invalid characters")
)

type Storage struct {
	root string
	opts *Options
}

func NewStorage(root string, opts ...OptionFunc) (*Storage, error) {
	// Copy default options to avoid mutating the global default.
	// The defaultOpts package-level variable must remain constant.
	options := &Options{
		FileMode:  defaultOpts.FileMode,
		DirMode:   defaultOpts.DirMode,
		ShardFunc: defaultOpts.ShardFunc,
	}

	// Apply user-provided options
	for _, opt := range opts {
		opt(options)
	}

	root = filepath.Clean(root)
	err := os.MkdirAll(filepath.Join(root, blobDirname), options.DirMode)
	if err != nil {
		return nil, fmt.Errorf("creating blobs directory: %w", err)
	}

	err = os.MkdirAll(filepath.Join(root, tempDirName), options.DirMode)
	if err != nil {
		return nil, fmt.Errorf("creating temp directory: %w", err)
	}

	return &Storage{
		root: root,
		opts: options,
	}, nil
}

// Put stores a blob with the given key by reading from the provided reader.
// If the key already exists, it will be overwritten while preserving the original
// creation timestamp.
//
// Why preserve createdAt: This maintains the original creation time even when
// a blob is updated, allowing tracking of when an object was first created vs.
// when it was last modified.
//
// Why temp file: Writing to a temporary file first ensures atomicity - the blob
// either fully exists or doesn't, preventing partial writes from being visible.
func (bs *Storage) Put(ctx context.Context, key string, r io.Reader) error {
	blob, err := bs.NewBlob()
	if err != nil {
		return err
	}
	defer blob.Discard()

	if _, err := io.Copy(blob, r); err != nil {
		return err
	}

	return blob.CommitAs(key)
}

func (bs *Storage) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := bs.validateKey(key); err != nil {
		return nil, err
	}

	storagePath := bs.createPathFromKey(key)
	dataPath := filepath.Join(storagePath, blobFileName)

	f, err := os.Open(dataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("blob %q: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("get blob %q: %w", key, err)
	}

	return f, nil
}

func (bs *Storage) newTempFile() (*os.File, error) {
	tempPath := filepath.Join(bs.root, tempDirName, newID())
	return os.Create(tempPath)
}

func (bs *Storage) Stat(ctx context.Context, key string) (*Meta, error) {
	if err := bs.validateKey(key); err != nil {
		return nil, err
	}

	storagePath := bs.createPathFromKey(key)

	meta, err := bs.readMeta(filepath.Join(storagePath, metaFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("blob %q: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("reading metadata for %q: %w", key, err)
	}

	return meta, nil
}

func (bs *Storage) Exists(ctx context.Context, key string) (bool, error) {
	if err := bs.validateKey(key); err != nil {
		return false, err
	}

	storagePath := bs.createPathFromKey(key)
	dataPath := filepath.Join(storagePath, blobFileName)

	_, err := os.Stat(dataPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

// BlobResult provides an iterator over blob storage entries.
// It follows the standard Go iterator pattern for streaming results.
//
// Why streaming: Loading all blobs into memory at once would be inefficient
// for large storages. The iterator pattern allows processing blobs one at a time.
//
// Why context in struct: While storing context in a struct is generally an
// anti-pattern, here it's necessary for the iterator to respect cancellation
// throughout its lifetime, not just at creation time.
type BlobResult struct {
	ctx    context.Context // This is an anti-pattern, but in this case it makes sense
	cancel context.CancelFunc

	// Channels for streaming results
	metaChan chan *Meta
	errChan  chan error

	// Current state
	current *Meta
	err     error
	closed  bool
}

// Next advances the iterator to the next blob.
// Returns true if there is a blob available, false if iteration is complete or an error occurred.
func (br *BlobResult) Next() bool {
	if br.closed || br.err != nil {
		return false
	}

	select {
	case meta, ok := <-br.metaChan:
		if !ok {
			// Channel closed, iteration complete
			return false
		}
		br.current = meta
		return true

	case err := <-br.errChan:
		br.err = err
		return false

	case <-br.ctx.Done():
		br.err = br.ctx.Err()
		return false
	}
}

// Meta returns the current blob's metadata.
// Only valid after Next() returns true.
func (br *BlobResult) Meta() *Meta {
	return br.current
}

// Key returns the current blob's key.
// Only valid after Next() returns true.
func (br *BlobResult) Key() string {
	if br.current == nil {
		return ""
	}
	return br.current.Key
}

// Err returns any error that occurred during iteration.
// Should be checked after Next() returns false.
func (br *BlobResult) Err() error {
	return br.err
}

// Close stops the iteration and releases resources.
// It's safe to call multiple times.
func (br *BlobResult) Close() error {
	if br.closed {
		return nil
	}

	br.closed = true
	if br.cancel != nil {
		br.cancel()
	}
	return nil
}

// List returns an iterator over all blobs matching the given prefix.
// The prefix is matched against the original blob keys, not the hashed storage paths.
// An empty prefix matches all blobs.
//
// The iterator must be closed when done to prevent resource leaks:
//
//	iter := storage.List(ctx, "prefix/")
//	defer iter.Close()
//	for iter.Next() {
//	    meta := iter.Meta()
//	    // process meta...
//	}
//	if err := iter.Err(); err != nil {
//	    // handle error
//	}
func (bs *Storage) List(ctx context.Context, prefix string) *BlobResult {
	// Create cancellable context
	ctx, cancel := context.WithCancel(ctx)

	result := &BlobResult{
		ctx:      ctx,
		cancel:   cancel,
		metaChan: make(chan *Meta, 10), // Buffer for smoother iteration
		errChan:  make(chan error, 1),
	}

	// Start goroutine to walk filesystem
	go bs.walkBlobs(ctx, prefix, result.metaChan, result.errChan)

	return result
}

// walkBlobs walks the blob storage directory and sends matching blobs to the channel.
func (bs *Storage) walkBlobs(ctx context.Context, prefix string, metaChan chan<- *Meta, errChan chan<- error) {
	defer close(metaChan)
	defer close(errChan)

	blobsDir := filepath.Join(bs.root, blobDirname)

	err := filepath.WalkDir(blobsDir, func(path string, d os.DirEntry, err error) error {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return err
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Only process meta.json files
		if d.Name() != metaFileName {
			return nil
		}

		// Read metadata
		meta, err := bs.readMeta(path)
		if err != nil {
			// Log but don't fail entire iteration for corrupted metadata
			return nil
		}

		// Check prefix match
		if prefix != "" && !strings.HasPrefix(meta.Key, prefix) {
			return nil
		}

		// Send to channel
		select {
		case metaChan <- meta:
		case <-ctx.Done():
			return ctx.Err()
		}

		return nil
	})

	// Send error if walk failed
	if err != nil && !errors.Is(err, context.Canceled) {
		select {
		case errChan <- err:
		case <-ctx.Done():
		}
	}
}

func (bs *Storage) Delete(ctx context.Context, key string) error {
	if err := bs.validateKey(key); err != nil {
		return err
	}

	storagePath := bs.createPathFromKey(key)

	// Idempotent: RemoveAll doesn't fail if path doesn't exist
	if err := os.RemoveAll(storagePath); err != nil {
		return fmt.Errorf("delete blob %q: %w", key, err)
	}

	// Clean up empty parent directories
	bs.cleanupEmptyDirs(storagePath)

	return nil
}

// createPathFromKey generates a storage path from a blob key using the configured
// ShardFunc. The default two-level directory structure (256^2 = 65,536 buckets) prevents
// filesystem performance degradation with large numbers of files.
func (bs *Storage) createPathFromKey(key string) string {
	shardPath := bs.opts.ShardFunc(key)
	return filepath.Join(bs.root, blobDirname, shardPath)
}

func (bs *Storage) readMeta(path string) (*Meta, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	var meta Meta
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decoding metadata: %w", err)
	}

	return &meta, nil
}

func (bs *Storage) validateKey(key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	if len(key) > maxKeyLength {
		return ErrKeyLengthExceeds
	}

	if filepath.IsAbs(key) {
		return fmt.Errorf("absolute paths are not allowed: %w", ErrInvalidKey)
	}

	// Check for path traversal attempts
	cleaned := filepath.Clean(key)
	if cleaned != key && cleaned != filepath.ToSlash(key) {
		return fmt.Errorf("path traversal detected: %w", ErrInvalidKey)
	}

	// Prevent directory traversal with ..
	if strings.Contains(key, "..") {
		return fmt.Errorf("relative path traversal not allowed: %w", ErrInvalidKey)
	}

	// Check for null bytes (security risk)
	if strings.Contains(key, "\x00") {
		return fmt.Errorf("null bytes not allowed: %w", ErrInvalidKey)
	}

	// Check for invalid characters
	for i, r := range key {
		if !isValidKeyChar(r) {
			return fmt.Errorf("invalid character %q at position %d: %w", r, i, ErrInvalidKey)
		}
	}

	// Prevent keys starting or ending with slash
	if strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") {
		return fmt.Errorf("key cannot start or end with slash: %w", ErrInvalidKey)
	}

	// Prevent consecutive slashes
	if strings.Contains(key, "//") {
		return fmt.Errorf("consecutive slashes not allowed: %w", ErrInvalidKey)
	}

	return nil
}

// isValidKeyChar reports whether r is a valid character in a blob key.
// Valid characters are: alphanumeric, hyphen, underscore, dot, forward slash.
func isValidKeyChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
		r == '-' || r == '_' || r == '.' || r == '/'
}

// cleanupEmptyDirs removes empty parent directories after blob deletion.
// It walks up the directory tree from the given path and removes empty
// directories until it reaches the blobs directory or finds a non-empty directory.
//
// Why cleanup: Prevents accumulation of empty directories over time, which can
// clutter the filesystem and slow down directory traversal operations.
//
// Why stop at non-empty: Ensures we don't accidentally remove directories that
// still contain other blobs.
func (bs *Storage) cleanupEmptyDirs(path string) {
	blobsDir := filepath.Join(bs.root, blobDirname)
	parent := filepath.Dir(path)

	// Walk up the tree until we reach the blobs directory
	for parent != blobsDir && parent != bs.root && parent != "." && parent != "/" {
		// Check if directory is empty
		entries, err := os.ReadDir(parent)
		if err != nil || len(entries) > 0 {
			// Directory doesn't exist, can't read it, or it's not empty - stop cleanup
			break
		}

		// Directory is empty, remove it
		if err := os.Remove(parent); err != nil {
			// Failed to remove, stop cleanup (don't want to fail the delete operation)
			break
		}

		// Move up to the next parent
		parent = filepath.Dir(parent)
	}
}
