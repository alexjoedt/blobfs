package blobfs

import "time"

// Meta contains metadata about a stored blob.
// This is stored separately from the blob data to enable
// efficient metadata queries without reading the blob content.
type Meta struct {
	Key         string    `json:"key"`                    // Original user-provided key
	Size        int64     `json:"size"`                   // Original (uncompressed) size in bytes
	StoredSize  int64     `json:"storedSize"`             // Size on disk (equals Size when uncompressed)
	Sha256      string    `json:"sha256"`                 // SHA-256 hash of original (uncompressed) content
	ContentType string    `json:"contentType"`            // MIME type detected from content
	Compression string    `json:"compression,omitempty"` // Compression codec name, empty if none
	CreatedAt   time.Time `json:"createdAt"`              // Original creation timestamp (preserved on update)
	ModifiedAt  time.Time `json:"modifiedAt"`             // Last modification timestamp
}
