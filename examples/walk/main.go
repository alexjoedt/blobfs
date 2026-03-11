// Example walk demonstrates iterating over stored blobs using Storage.Walk.
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/alexjoedt/blobfs"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "blobfs-walk-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	storage, err := blobfs.NewStorage(dir)
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Seed some blobs with different prefixes
	blobs := map[string]string{
		"images/photo1.jpg":     "jpeg-data-1",
		"images/photo2.jpg":     "jpeg-data-2",
		"images/icons/logo.png": "png-data",
		"documents/report.pdf":  "pdf-data",
		"documents/notes.txt":   "text-data",
		"readme.md":             "# Hello",
	}

	for key, content := range blobs {
		if err := storage.Put(ctx, key, bytes.NewReader([]byte(content))); err != nil {
			return err
		}
	}
	fmt.Fprintf(os.Stdout, "stored %d blobs\n\n", len(blobs))

	// Walk all blobs
	fmt.Fprintln(os.Stdout, "=== All blobs ===")
	var count int
	err = storage.Walk(ctx, "", func(key string, meta *blobfs.Meta, err error) error {
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "  %s (%d bytes, %s)\n", key, meta.Size, meta.ContentType)
		count++
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "total: %d\n\n", count)

	// Walk only images
	fmt.Fprintln(os.Stdout, "=== Images only (prefix: images/) ===")
	err = storage.Walk(ctx, "images/", func(key string, meta *blobfs.Meta, err error) error {
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "  %s (%d bytes)\n", key, meta.Size)
		return nil
	})
	if err != nil {
		return err
	}

	// Walk with early termination using filepath.SkipAll
	fmt.Fprintln(os.Stdout, "\n=== First 2 blobs (early stop) ===")
	var seen int
	err = storage.Walk(ctx, "", func(key string, _ *blobfs.Meta, err error) error {
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "  %s\n", key)
		seen++
		if seen >= 2 {
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "stopped after %d blobs\n", seen)

	return nil
}
