package blobfs

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

var (
	ErrBlobClosed   = errors.New("blob is closed")
	ErrBlobNotReady = errors.New("blob not ready for finalization")
)

// Blob represents a writable blob in the storage.
// It implements [io.Writer] and [io.Closer], allowing it to be used with [io.Copy]
// and other standard Go interfaces.
//
// Why separate temp file: Using a temporary file during writes allows atomic
// creation - the blob either fully exists or doesn't, preventing partial writes
// from being visible. This is critical for data consistency.
//
// Why hash during write: Computing the hash while writing avoids reading the
// entire file again after writing, which would be inefficient for large files.
//
// Example usage:
//
//	blob, err := storage.NewBlob()
//	if err != nil {
//		return err
//	}
//	defer blob.Discard() // Safety: cleanup if not committed
//
//	if _, err = io.Copy(blob, sourceReader); err != nil {
//		return err
//	}
//
//	// Commit with specific key (e.g., content hash for CAS)
//	return blob.CommitAs(blob.Hash())
type Blob struct {
	storage *Storage

	// Temp file handling
	// Temporary files ensure atomic blob creation - the blob is either
	// fully written or doesn't exist at all.
	tmpFile *os.File
	tmpPath string

	// Content tracking
	// SHA-256 is computed incrementally during writes to avoid
	// re-reading large files after writing.
	hasher hash.Hash
	size   int64

	// Content type detection buffer
	// Buffers the first 512 bytes for http.DetectContentType
	// rather than storing entire file content in memory.
	buffer     []byte
	bufferUsed int

	contentType string

	// Compression
	compressWriter io.WriteCloser // non-nil when compression is active
	storedSize     int64          // bytes actually written to tmpFile (after compression)

	// Metadata
	// Cached after commit to avoid re-reading the meta file.
	meta *Meta

	// State management
	mu        sync.Mutex
	closed    bool
	committed bool
	err       error // Sticky error for failed blobs
}

// NewBlob creates a new writable blob with a temporary internal ID.
// The blob must be explicitly committed with CommitAs(key) to persist it,
// or discarded with Discard() or Close() to clean up the temporary file.
//
// The blob automatically:
//   - Creates a temporary file for writing
//   - Computes SHA-256 hash while writing
//   - Detects content type from the first 512 bytes
//
// Example usage:
//
//	blob, err := storage.NewBlob()
//	if err != nil {
//		return err
//	}
//	defer blob.Discard() // Safety: cleanup if we don't commit
//
//	io.Copy(blob, reader)
//	hash := blob.Hash()
//
//	// Check if blob with this hash already exists
//	if exists, _ := storage.Exists(ctx, hash); exists {
//		return nil // Already stored
//	}
//
//	// Commit with hash as key
//	return blob.CommitAs(hash)
func (bs *Storage) NewBlob() (*Blob, error) {
	// Create temp file
	tmpFile, err := bs.newTempFile()
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}

	bufferSize := 512
	blob := &Blob{
		storage: bs,
		tmpFile: tmpFile,
		tmpPath: tmpFile.Name(),
		hasher:  sha256.New(),
		buffer:  make([]byte, bufferSize),
	}

	switch bs.opts.Compression {
	case CodecGzip:
		blob.compressWriter = gzip.NewWriter(tmpFile)
	case CodecZstd:
		zw, zstdErr := zstd.NewWriter(tmpFile)
		if zstdErr != nil {
			_ = tmpFile.Close()
			return nil, fmt.Errorf("creating zstd writer: %w", zstdErr)
		}
		blob.compressWriter = zw
	case CodecNone:
		// no compression
	default:
		return nil, ErrInvalidCodec
	}

	return blob, nil
}

// Write implements [io.Writer], writing data to the blob.
// The first 512 bytes are buffered for content type detection.
//
// Why buffer first 512 bytes: [http.DetectContentType] needs up to 512 bytes
// to accurately determine MIME type from file signatures, but we don't want
// to buffer the entire file in memory for large files.
func (b *Blob) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return 0, ErrBlobClosed
	}

	if b.err != nil {
		return 0, b.err
	}

	// Buffer first bytes for content type detection
	if b.bufferUsed < len(b.buffer) {
		toCopy := min(len(b.buffer)-b.bufferUsed, len(p))
		copy(b.buffer[b.bufferUsed:], p[:toCopy])
		b.bufferUsed += toCopy
	}

	if b.compressWriter != nil {
		// Hash original (uncompressed) bytes before compression.
		if _, err := b.hasher.Write(p); err != nil {
			b.err = fmt.Errorf("hashing write: %w", err)
			return 0, b.err
		}
		written, err := b.compressWriter.Write(p)
		if err != nil {
			b.err = fmt.Errorf("writing compressed data: %w", err)
			return written, b.err
		}
		b.size += int64(len(p))
		return len(p), nil
	}

	// Write to temp file and hasher
	written, err := b.tmpFile.Write(p)
	if err != nil {
		b.err = fmt.Errorf("writing to temp file: %w", err)
		return written, b.err
	}

	// Update hash
	_, hashErr := b.hasher.Write(p[:written])
	if hashErr != nil {
		b.err = fmt.Errorf("hashing write: %w", hashErr)
		return written, b.err
	}

	b.size += int64(written)
	return written, nil
}

// Hash returns the computed SHA-256 hash of the blob content as a hex string.
// This can be called before committing to determine the content hash.
// Returns empty string if no data has been written yet.
func (b *Blob) Hash() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return hex.EncodeToString(b.hasher.Sum(nil))
}

// CommitAs finalizes the blob by committing it with the specified key.
// The blob is atomically moved from the temporary location to the final storage location.
//
// This method:
//  1. Validates the key
//  2. Detects content type from buffered data
//  3. Creates metadata
//  4. Atomically moves temp file to final location
//
// Returns ErrEmptyKey if key is empty, or ErrBlobClosed if already closed/committed.
// After successful commit, the blob is closed and cannot be reused.
//
// Why atomic move: [os.Rename] is atomic on most filesystems, ensuring the blob
// either fully exists with metadata or doesn't exist at all. This prevents
// other processes from reading partial or inconsistent data.
func (b *Blob) CommitAs(key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBlobClosed
	}

	if b.err != nil {
		return b.err
	}

	if key == "" {
		b.err = ErrEmptyKey
		return b.err
	}

	// Validate key
	if err := b.storage.validateKey(key); err != nil {
		b.err = err
		return b.err
	}

	b.closed = true
	b.committed = true

	if err := b.finalizeTempFile(); err != nil {
		b.err = err
		return b.err
	}

	// Clean up on error
	defer func() {
		if b.err != nil && b.tmpPath != "" {
			_ = os.Remove(b.tmpPath)
		}
	}()

	// Detect content type from buffered data
	b.contentType = http.DetectContentType(b.buffer[:b.bufferUsed])

	// Compute final hash
	contentHash := hex.EncodeToString(b.hasher.Sum(nil))

	// Check if key exists to preserve createdAt
	createdAt := time.Now()
	storagePath := b.storage.createPathFromKey(key)
	metaPath := filepath.Join(storagePath, metaFileName)
	if existingMeta, err := b.storage.readMeta(metaPath); err == nil {
		createdAt = existingMeta.CreatedAt
	}

	// Create metadata
	meta := &Meta{
		Key:         key,
		Size:        b.size,
		StoredSize:  b.storedSize,
		Sha256:      contentHash,
		ContentType: b.contentType,
		Compression: string(b.storage.opts.Compression),
		CreatedAt:   createdAt,
		ModifiedAt:  time.Now(),
	}

	// Store metadata for later access
	b.meta = meta

	// Create storage directory
	if err := os.MkdirAll(storagePath, b.storage.opts.DirMode); err != nil {
		b.err = err
		return b.err
	}

	// Write metadata before moving the blob to ensure consistency.
	if err := b.storage.writeMeta(meta, metaPath); err != nil {
		b.err = err
		return b.err
	}

	// Commit data into the object store and hard-link into refs.
	dataPath := filepath.Join(storagePath, blobFileName)
	err := b.storage.commitData(b.tmpPath, dataPath, contentHash)
	if err != nil {
		b.err = fmt.Errorf("committing blob: %w", err)
		return b.err
	}

	return nil
}

// finalizeTempFile flushes the compress writer (if any), records the on-disk
// size, and closes the temporary file. It is called once from CommitAs.
func (b *Blob) finalizeTempFile() error {
	if b.compressWriter != nil {
		if err := b.compressWriter.Close(); err != nil {
			return fmt.Errorf("closing compress writer: %w", err)
		}
	}
	if info, err := b.tmpFile.Stat(); err == nil {
		b.storedSize = info.Size()
	}
	if b.tmpFile != nil {
		if err := b.tmpFile.Close(); err != nil {
			return fmt.Errorf("closing temp file: %w", err)
		}
	}
	return nil
}

// Discard closes the blob and removes the temporary file without committing.
// This is safe to call even if the blob has already been closed or committed.
// Idempotent - safe to call multiple times.
func (b *Blob) Discard() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil // Already closed/discarded
	}

	b.closed = true

	// Close temp file
	if b.tmpFile != nil {
		if err := b.tmpFile.Close(); err != nil && b.err == nil {
			b.err = err
		}
	}

	// Remove temp file
	if b.tmpPath != "" {
		_ = os.Remove(b.tmpPath) // Ignore error - best effort cleanup
	}

	return b.err
}

// Close is an alias for Discard(). It closes the blob and removes the temporary file
// without committing. To persist the blob, use CommitAs() instead.
//
// This allows Blob to satisfy [io.Closer] for compatibility with defer patterns,
// but does NOT commit the blob to storage.
func (b *Blob) Close() error {
	return b.Discard()
}

// Size returns the current size of the blob in bytes.
// This can be called before committing to get the current written size.
func (b *Blob) Size() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}

// Closed returns true if the blob has been closed or committed.
func (b *Blob) Closed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// Meta returns the metadata of the committed blob.
// Returns nil if the blob has not been successfully committed yet.
// This allows access to metadata without re-reading the meta file.
func (b *Blob) Meta() *Meta {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.meta
}
