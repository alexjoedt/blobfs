package blobfs

import (
	"errors"
	"testing"
)

func TestValidateKey(t *testing.T) {
	bs := &BlobStorage{root: "./test"}

	tests := []struct {
		name    string
		key     string
		wantErr error
	}{
		// Valid keys
		{
			name:    "simple key",
			key:     "test.txt",
			wantErr: nil,
		},
		{
			name:    "nested path",
			key:     "users/john/documents/invoice.pdf",
			wantErr: nil,
		},
		{
			name:    "with hyphens and underscores",
			key:     "my-file_v2.txt",
			wantErr: nil,
		},
		{
			name:    "with numbers",
			key:     "report-2024-11-05.pdf",
			wantErr: nil,
		},

		// Invalid keys - empty
		{
			name:    "empty key",
			key:     "",
			wantErr: ErrEmptyKey,
		},

		// Invalid keys - length
		{
			name:    "key too long",
			key:     string(make([]byte, maxKeyLength+1)),
			wantErr: ErrKeyLengthExceeds,
		},

		// Invalid keys - absolute paths
		{
			name:    "absolute path unix",
			key:     "/etc/passwd",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "absolute path windows",
			key:     "C:/Users/test.txt",
			wantErr: ErrInvalidKey,
		},

		// Invalid keys - path traversal
		{
			name:    "parent directory traversal",
			key:     "../../../etc/passwd",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "path traversal in middle",
			key:     "users/../admin/secrets.txt",
			wantErr: ErrInvalidKey,
		},

		// Invalid keys - null bytes
		{
			name:    "null byte",
			key:     "test\x00.txt",
			wantErr: ErrInvalidKey,
		},

		// Invalid keys - special characters
		{
			name:    "spaces",
			key:     "my file.txt",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "asterisk",
			key:     "*.txt",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "question mark",
			key:     "file?.txt",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "backslash",
			key:     "path\\to\\file.txt",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "pipe",
			key:     "file|name.txt",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "angle brackets",
			key:     "<script>.txt",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "colon",
			key:     "file:name.txt",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "quotes",
			key:     "file\"name.txt",
			wantErr: ErrInvalidKey,
		},

		// Invalid keys - slash issues
		{
			name:    "leading slash",
			key:     "/users/test.txt",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "trailing slash",
			key:     "users/test.txt/",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "consecutive slashes",
			key:     "users//john/test.txt",
			wantErr: ErrInvalidKey,
		},

		// Invalid keys - control characters
		{
			name:    "newline",
			key:     "file\nname.txt",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "tab",
			key:     "file\tname.txt",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "carriage return",
			key:     "file\rname.txt",
			wantErr: ErrInvalidKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := bs.validateKey(tt.key)
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("validateKey() error = %v, wantErr nil", err)
				}
			} else {
				if err == nil {
					t.Errorf("validateKey() expected error containing %v, got nil", tt.wantErr)
				} else if !errors.Is(err, tt.wantErr) {
					t.Errorf("validateKey() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}
