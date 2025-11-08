package blobfs

import "time"

// Meta contains metadata about a stored blob.
// This is stored separately from the blob data to enable
// efficient metadata queries without reading the blob content.
type Meta struct {
	Key         string    `json:"key"`         // Original user-provided key
	Size        int64     `json:"size"`        // Size in bytes
	Sha256      string    `json:"sha256"`      // SHA-256 hash of content
	ContentType string    `json:"contentType"` // MIME type detected from content
	CreatedAt   time.Time `json:"createdAt"`   // Original creation timestamp (preserved on update)
	ModifiedAt  time.Time `json:"modifiedAt"`  // Last modification timestamp
}
