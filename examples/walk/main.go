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
	dir, err := os.MkdirTemp("", "blobfs-walk-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage, err := blobfs.NewStorage(dir)
	if err != nil {
		log.Fatal(err)
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
			log.Fatal(err)
		}
	}
	fmt.Printf("stored %d blobs\n\n", len(blobs))

	// Walk all blobs
	fmt.Println("=== All blobs ===")
	var count int
	err = storage.Walk(ctx, "", func(key string, meta *blobfs.Meta, err error) error {
		if err != nil {
			return err
		}
		fmt.Printf("  %s (%d bytes, %s)\n", key, meta.Size, meta.ContentType)
		count++
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("total: %d\n\n", count)

	// Walk only images
	fmt.Println("=== Images only (prefix: images/) ===")
	err = storage.Walk(ctx, "images/", func(key string, meta *blobfs.Meta, err error) error {
		if err != nil {
			return err
		}
		fmt.Printf("  %s (%d bytes)\n", key, meta.Size)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Walk with early termination using filepath.SkipAll
	fmt.Println("\n=== First 2 blobs (early stop) ===")
	var seen int
	err = storage.Walk(ctx, "", func(key string, meta *blobfs.Meta, err error) error {
		if err != nil {
			return err
		}
		fmt.Printf("  %s\n", key)
		seen++
		if seen >= 2 {
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("stopped after %d blobs\n", seen)
}
