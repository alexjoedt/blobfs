package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/alexjoedt/blobfs"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Create a temporary directory for the example
	dir, err := os.MkdirTemp("", "blobfs-basic-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// Initialize storage with default options
	storage, err := blobfs.NewStorage(dir)
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Store a blob
	content := []byte("Hello, BlobFS!")
	if err := storage.Put(ctx, "greetings/hello.txt", bytes.NewReader(content)); err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, "stored blob: greetings/hello.txt")

	// Check if a blob exists
	exists, err := storage.Exists(ctx, "greetings/hello.txt")
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "exists: %v\n", exists)

	// Get metadata without reading the content
	meta, err := storage.Stat(ctx, "greetings/hello.txt")
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "size: %d bytes, content-type: %s, sha256: %s\n", meta.Size, meta.ContentType, meta.Sha256)

	// Read the blob back
	rc, err := storage.Get(ctx, "greetings/hello.txt")
	if err != nil {
		return err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "content: %s\n", data)

	// Delete the blob
	if err := storage.Delete(ctx, "greetings/hello.txt"); err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, "deleted blob: greetings/hello.txt")

	// Verify deletion
	exists, err = storage.Exists(ctx, "greetings/hello.txt")
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "exists after delete: %v\n", exists)

	return nil
}
