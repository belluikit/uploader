// Package storage provides a file storage backend for the upload service.
package storage

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Backend defines the server-side file storage abstraction.
type Backend interface {
	// CreateUpload initializes temp storage for a new upload and returns a unique file ID.
	CreateUpload(filename string) (fileID string, err error)

	// WriteChunk writes data at the given byte offset for an in-progress upload.
	WriteChunk(fileID string, offset int64, data []byte) error

	// CommittedSize returns the number of bytes written so far to a staging file.
	CommittedSize(fileID string) (int64, error)

	// TruncateTo truncates the staging file to the given size.
	TruncateTo(fileID string, size int64) error

	// FinalizeUpload moves the staging file to permanent storage. Finalizing
	// an upload that is already final is not an error.
	FinalizeUpload(fileID string) error

	// OpenFile opens a finalized file for reading starting at offset.
	// Returns a ReadCloser, the total file size, and any error.
	OpenFile(fileID string, offset int64) (io.ReadCloser, int64, error)

	// FileHash computes and returns the SHA-256 hash of a finalized file.
	FileHash(fileID string) ([]byte, error)

	// DeleteStaging removes the staging file for an upload.
	DeleteStaging(fileID string) error

	// Delete removes a finalized file.
	Delete(fileID string) error
}

// Filesystem implements Backend using the local filesystem.
type Filesystem struct {
	staging string // directory for in-progress uploads
	files   string // directory for finalized files
}

// NewFilesystem creates a filesystem backend rooted at baseDir.
// It creates staging/ and files/ subdirectories.
func NewFilesystem(baseDir string) (*Filesystem, error) {
	staging := filepath.Join(baseDir, "staging")
	files := filepath.Join(baseDir, "files")
	for _, d := range []string{staging, files} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("create dir %s: %w", d, err)
		}
	}
	return &Filesystem{staging: staging, files: files}, nil
}

func (fs *Filesystem) CreateUpload(filename string) (string, error) {
	fileID := generateFileID()
	path, err := fs.stagingPath(fileID)
	if err != nil {
		return "", err
	}
	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("create staging file: %w", err)
	}
	f.Close()
	return fileID, nil
}

func (fs *Filesystem) WriteChunk(fileID string, offset int64, data []byte) error {
	path, err := fs.stagingPath(fileID)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open staging file: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteAt(data, offset); err != nil {
		return fmt.Errorf("write at offset %d: %w", offset, err)
	}
	return nil
}

func (fs *Filesystem) CommittedSize(fileID string) (int64, error) {
	path, err := fs.stagingPath(fileID)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat staging file: %w", err)
	}
	return info.Size(), nil
}

func (fs *Filesystem) TruncateTo(fileID string, size int64) error {
	path, err := fs.stagingPath(fileID)
	if err != nil {
		return err
	}
	return os.Truncate(path, size)
}

func (fs *Filesystem) FinalizeUpload(fileID string) error {
	src, err := fs.stagingPath(fileID)
	if err != nil {
		return err
	}
	dst, err := fs.filePath(fileID)
	if err != nil {
		return err
	}
	if err := os.Rename(src, dst); err != nil {
		// Already finalized: the staging file is gone and the final one exists.
		if errors.Is(err, os.ErrNotExist) {
			if _, serr := os.Stat(dst); serr == nil {
				return nil
			}
		}
		return fmt.Errorf("finalize upload: %w", err)
	}
	return nil
}

func (fs *Filesystem) OpenFile(fileID string, offset int64) (io.ReadCloser, int64, error) {
	path, err := fs.filePath(fileID)
	if err != nil {
		return nil, 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, 0, fmt.Errorf("stat file: %w", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open file: %w", err)
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return nil, 0, fmt.Errorf("seek to offset %d: %w", offset, err)
		}
	}
	return f, info.Size(), nil
}

func (fs *Filesystem) FileHash(fileID string) ([]byte, error) {
	path, err := fs.filePath(fileID)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file for hash: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, fmt.Errorf("hash file: %w", err)
	}
	return h.Sum(nil), nil
}

func (fs *Filesystem) DeleteStaging(fileID string) error {
	path, err := fs.stagingPath(fileID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (fs *Filesystem) Delete(fileID string) error {
	path, err := fs.filePath(fileID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ErrInvalidFileID is returned for a file ID this package did not generate.
// File IDs become path components, so only the exact form generateFileID
// produces is accepted; anything else (a path separator, "..", wrong length)
// could escape the staging and files directories.
var ErrInvalidFileID = errors.New("invalid file ID")

// ValidFileID reports whether id has the 32-lowercase-hex form that
// generateFileID produces.
func ValidFileID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func (fs *Filesystem) stagingPath(fileID string) (string, error) {
	if !ValidFileID(fileID) {
		return "", ErrInvalidFileID
	}
	return filepath.Join(fs.staging, fileID), nil
}

func (fs *Filesystem) filePath(fileID string) (string, error) {
	if !ValidFileID(fileID) {
		return "", ErrInvalidFileID
	}
	return filepath.Join(fs.files, fileID), nil
}

func generateFileID() string {
	b := make([]byte, 16)
	_, _ = io.ReadFull(rand.Reader, b)
	return hex.EncodeToString(b)
}
