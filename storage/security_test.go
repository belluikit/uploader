package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestValidFileID(t *testing.T) {
	good := generateFileID()
	if !ValidFileID(good) {
		t.Errorf("generated ID %q rejected", good)
	}
	for _, bad := range []string{
		"", "..", "../x", "a/b", "ABCDEF0011223344556677889900aabb", // uppercase
		"0011223344556677889900aabbccddee11", // 34 chars
		"0011223344556677889900aabbccdde",    // 31 chars
		"0011223344556677889900aabbccddeg",   // non-hex g
	} {
		if ValidFileID(bad) {
			t.Errorf("invalid ID %q accepted", bad)
		}
	}
}

// Every method that takes a file ID refuses a traversal ID, and nothing is
// created, read or removed outside the files/staging directories.
func TestStorageRejectsTraversalID(t *testing.T) {
	root := t.TempDir()
	// A secret two levels above the files dir (files = root/data/files).
	secret := filepath.Join(root, "secret.txt")
	if err := os.WriteFile(secret, []byte("s"), 0o600); err != nil {
		t.Fatal(err)
	}
	fs, err := NewFilesystem(filepath.Join(root, "data"))
	if err != nil {
		t.Fatal(err)
	}
	bad := "../../secret"

	if _, _, err := fs.OpenFile(bad, 0); !errors.Is(err, ErrInvalidFileID) {
		t.Errorf("OpenFile: %v, want ErrInvalidFileID", err)
	}
	if _, err := fs.FileHash(bad); !errors.Is(err, ErrInvalidFileID) {
		t.Errorf("FileHash: %v, want ErrInvalidFileID", err)
	}
	if err := fs.WriteChunk(bad, 0, []byte("x")); !errors.Is(err, ErrInvalidFileID) {
		t.Errorf("WriteChunk: %v, want ErrInvalidFileID", err)
	}
	if err := fs.FinalizeUpload(bad); !errors.Is(err, ErrInvalidFileID) {
		t.Errorf("FinalizeUpload: %v, want ErrInvalidFileID", err)
	}
	if err := fs.Delete("../../secret.txt"); !errors.Is(err, ErrInvalidFileID) {
		t.Errorf("Delete: %v, want ErrInvalidFileID", err)
	}
	// The secret is untouched.
	if _, err := os.Stat(secret); err != nil {
		t.Fatalf("secret file disturbed: %v", err)
	}
}
