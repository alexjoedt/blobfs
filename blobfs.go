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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	tempDirName    = ".tmp"
	refsDirName    = "refs"
	objectsDirName = "objects"
	metaFileName   = "meta.json"
	blobFileName   = "data"

	maxKeyLength = 1024
)

var (
	ErrKeyLengthExceeds = errors.New("maximal key length exceeds")
	ErrNotFound         = errors.New("blob with key not found")
	ErrEmptyKey         = errors.New("key cannot be empty")
	ErrInvalidKey       = errors.New("key contains invalid characters")
	ErrCorrupted        = errors.New("blob content does not match stored hash")
)

type Storage struct {
	root       string
	opts       *Options
	refsDir    string // absolute path to refs/
	objectsDir string // absolute path to objects/
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

	// Create refs directory (keyed by SHA-256 of user key)
	refsDir := filepath.Join(root, refsDirName)
	if err := os.MkdirAll(refsDir, options.DirMode); err != nil {
		return nil, fmt.Errorf("creating refs directory: %w", err)
	}

	// Create objects directory (keyed by SHA-256 of content — CAS store)
	objectsDir := filepath.Join(root, objectsDirName)
	if err := os.MkdirAll(objectsDir, options.DirMode); err != nil {
		return nil, fmt.Errorf("creating objects directory: %w", err)
	}

	// Create temp directory (always at root level)
	if err := os.MkdirAll(filepath.Join(root, tempDirName), options.DirMode); err != nil {
		return nil, fmt.Errorf("creating temp directory: %w", err)
	}

	return &Storage{
		root:       root,
		opts:       options,
		refsDir:    refsDir,
		objectsDir: objectsDir,
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

// Open returns a ReadSeekCloser for the blob identified by key.
// The caller is responsible for closing the returned value.
// Use this instead of Get when you need seeking or HTTP range serving:
//
//	f, err := storage.Open(ctx, "videos/clip.mp4")
//	if err != nil { ... }
//	defer f.Close()
//	http.ServeContent(w, r, "clip.mp4", meta.ModifiedAt, f)
func (bs *Storage) Open(ctx context.Context, key string) (io.ReadSeekCloser, error) {
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
		return nil, fmt.Errorf("open blob %q: %w", key, err)
	}

	return f, nil
}

func (bs *Storage) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := bs.Open(ctx, key)
	if err != nil {
		return nil, err
	}

	if bs.opts.VerifyOnRead {
		meta, err := bs.Stat(ctx, key)
		if err != nil {
			_ = rc.Close()
			return nil, err
		}
		return newVerifyReader(rc, meta.Sha256), nil
	}

	return rc, nil
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

// verifyReader wraps an io.ReadCloser and computes a hash as data is read.
// On Close, it verifies that the computed hash matches the expected value,
// returning ErrCorrupted if they don't match.
type verifyReader struct {
	rc       io.ReadCloser
	hasher   hash.Hash
	expected string // hex-encoded SHA-256 from meta
}

// newVerifyReader creates a new verifyReader that wraps rc.
// The expected hash should be the hex-encoded SHA-256 value from meta.Sha256.
func newVerifyReader(rc io.ReadCloser, expected string) *verifyReader {
	return &verifyReader{
		rc:       rc,
		hasher:   sha256.New(),
		expected: expected,
	}
}

// Read reads from the underlying reader while computing the hash.
func (v *verifyReader) Read(p []byte) (int, error) {
	n, err := v.rc.Read(p)
	if n > 0 {
		_, _ = v.hasher.Write(p[:n])
	}
	return n, err
}

// Close closes the underlying reader and verifies the computed hash
// against the expected value. Returns ErrCorrupted if they don't match.
func (v *verifyReader) Close() error {
	if err := v.rc.Close(); err != nil {
		return err
	}
	got := hex.EncodeToString(v.hasher.Sum(nil))
	if got != v.expected {
		return fmt.Errorf("%w: stored=%s actual=%s", ErrCorrupted, v.expected, got)
	}
	return nil
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
//
// Deprecated: Use [Storage.Walk] instead. BlobResult and List will be removed in a future version.
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

// WalkFn is the callback signature for [Storage.Walk].
// Return [filepath.SkipAll] to stop iteration early without an error.
// Any other non-nil return value aborts the walk and is returned by Walk.
type WalkFn func(key string, meta *Meta, err error) error

// Walk calls fn for each blob whose key has the given prefix, in filesystem order.
// An empty prefix matches all blobs.
// Return [filepath.SkipAll] from fn to stop early without error.
//
// Unlike [Storage.List], Walk is synchronous and requires no cleanup.
//
// Example:
//
//	err := storage.Walk(ctx, "documents/", func(key string, meta *Meta, err error) error {
//		if err != nil {
//			return err
//		}
//		fmt.Println(key, meta.Size)
//		return nil
//	})
func (bs *Storage) Walk(ctx context.Context, prefix string, fn WalkFn) error {
	return filepath.WalkDir(bs.refsDir, func(path string, d os.DirEntry, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		if d.Name() != metaFileName {
			return nil
		}

		meta, err := bs.readMeta(path)
		if err != nil {
			return fn("", nil, fmt.Errorf("reading metadata at %q: %w", path, err))
		}

		if prefix != "" && !strings.HasPrefix(meta.Key, prefix) {
			return nil
		}

		return fn(meta.Key, meta, nil)
	})
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
	return filepath.Join(bs.refsDir, shardPath)
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

// commitData places the content from tmpPath into the object store and creates
// a hard link at dataPath. The first write of a given contentHash moves the
// temp file into objects/; subsequent writes with the same hash discard the
// temp file and link to the already-stored object.
//
// Hard links keep the inode reference count accurate, enabling GC to detect
// unreferenced objects by looking for nlink == 1.
func (bs *Storage) commitData(tmpPath, dataPath, contentHash string) error {
	objectPath := bs.objectPath(contentHash)

	if _, err := os.Stat(objectPath); errors.Is(err, os.ErrNotExist) {
		// First time this content is seen: move tmp into the object store.
		if err := os.MkdirAll(filepath.Dir(objectPath), bs.opts.DirMode); err != nil {
			return err
		}
		if err := os.Rename(tmpPath, objectPath); err != nil {
			return err
		}
	} else {
		// Content already exists: discard the temp file.
		_ = os.Remove(tmpPath)
	}

	// Hard-link the object into the ref slot (remove stale link first on overwrite).
	_ = os.Remove(dataPath)
	if err := os.Link(objectPath, dataPath); err != nil {
		// Fallback for network filesystems or cross-device edge cases.
		return copyFile(objectPath, dataPath, bs.opts.FileMode)
	}
	return nil
}

// objectPath returns the canonical path for a content object identified by its
// hex-encoded SHA-256 hash.
func (bs *Storage) objectPath(contentHash string) string {
	s1, s2 := contentHash[:2], contentHash[2:4]
	return filepath.Join(bs.objectsDir, s1, s2, contentHash)
}

// copyFile copies src to dst with the given mode. Used as a fallback when
// os.Link fails (e.g. cross-device or network filesystem).
func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
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
	parent := filepath.Dir(path)

	// Walk up the tree until we reach the refs directory
	for parent != bs.refsDir && parent != bs.root && parent != "." && parent != "/" {
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

// MigrateStats reports the outcome of a Migrate call.
type MigrateStats struct {
	BlobsScanned int
	BlobsLinked  int   // blobs whose data file was converted to a hard link
	BlobsSkipped int   // blobs already hard-linked (idempotent re-runs)
	BytesSaved   int64 // bytes freed (storedSize of de-duplicated blobs)
}

// Migrate retroactively populates objects/ from existing refs and converts their
// data files to hard links. Safe to run on a live store; each blob is processed
// atomically. Idempotent — already-migrated blobs are skipped.
//
// Use this after upgrading an existing store that was created before the CAS
// object store was introduced, so that new writes of identical content can
// deduplicate against the migrated objects.
func (bs *Storage) Migrate(ctx context.Context) (MigrateStats, error) {
	var stats MigrateStats

	err := bs.Walk(ctx, "", func(key string, meta *Meta, err error) error {
		if err != nil {
			return err
		}
		stats.BlobsScanned++

		storagePath := bs.createPathFromKey(key)
		dataPath := filepath.Join(storagePath, blobFileName)
		objectPath := bs.objectPath(meta.Sha256)

		info, err := os.Lstat(dataPath)
		if err != nil {
			return fmt.Errorf("stat %q: %w", dataPath, err)
		}

		objInfo, objErr := os.Stat(objectPath)

		if objErr == nil {
			// Object already exists. Check whether dataPath is already a hard link
			// to the same object by comparing file identity.
			if os.SameFile(info, objInfo) {
				// Already a hard link — nothing to do.
				stats.BlobsSkipped++
				return nil
			}
			// Object exists but data is a separate copy: replace with hard link.
			_ = os.Remove(dataPath)
			if linkErr := os.Link(objectPath, dataPath); linkErr != nil {
				if copyErr := copyFile(objectPath, dataPath, bs.opts.FileMode); copyErr != nil {
					return fmt.Errorf("linking %q: %w", dataPath, linkErr)
				}
			}
			stats.BlobsLinked++
			stats.BytesSaved += info.Size()
			return nil
		}

		if !errors.Is(objErr, os.ErrNotExist) {
			return fmt.Errorf("stat object %q: %w", objectPath, objErr)
		}

		// Object does not exist yet: create the directory and hard-link the data
		// file as the object, then re-link dataPath to the object so both share
		// the same inode.
		if mkErr := os.MkdirAll(filepath.Dir(objectPath), bs.opts.DirMode); mkErr != nil {
			return fmt.Errorf("creating object dir: %w", mkErr)
		}
		if linkErr := os.Link(dataPath, objectPath); linkErr != nil {
			if copyErr := copyFile(dataPath, objectPath, bs.opts.FileMode); copyErr != nil {
				return fmt.Errorf("creating object for %q: %w", key, linkErr)
			}
		}
		stats.BlobsLinked++
		return nil
	})

	return stats, err
}
