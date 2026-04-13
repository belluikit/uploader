package storage

import (
	"bytes"
	"crypto/sha256"
	"io"
	"testing"
)

func TestFilesystemUploadCycle(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFilesystem(dir)
	if err != nil {
		t.Fatal(err)
	}

	fileID, err := fs.CreateUpload("test.txt")
	if err != nil {
		t.Fatal("create:", err)
	}

	// Write chunks.
	chunk1 := []byte("hello ")
	chunk2 := []byte("world!")
	if err := fs.WriteChunk(fileID, 0, chunk1); err != nil {
		t.Fatal("write chunk1:", err)
	}
	if err := fs.WriteChunk(fileID, int64(len(chunk1)), chunk2); err != nil {
		t.Fatal("write chunk2:", err)
	}

	// Verify committed size.
	size, err := fs.CommittedSize(fileID)
	if err != nil {
		t.Fatal("committed size:", err)
	}
	if size != int64(len(chunk1)+len(chunk2)) {
		t.Errorf("size: got %d, want %d", size, len(chunk1)+len(chunk2))
	}

	// Finalize.
	if err := fs.FinalizeUpload(fileID); err != nil {
		t.Fatal("finalize:", err)
	}

	// Read back.
	reader, totalSize, err := fs.OpenFile(fileID, 0)
	if err != nil {
		t.Fatal("open:", err)
	}
	defer reader.Close()

	if totalSize != int64(len(chunk1)+len(chunk2)) {
		t.Errorf("totalSize: got %d, want %d", totalSize, len(chunk1)+len(chunk2))
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal("read:", err)
	}

	expected := append(chunk1, chunk2...)
	if !bytes.Equal(data, expected) {
		t.Errorf("content: got %q, want %q", data, expected)
	}
}

func TestFilesystemOpenFileWithOffset(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFilesystem(dir)
	if err != nil {
		t.Fatal(err)
	}

	fileID, _ := fs.CreateUpload("offset.txt")
	content := []byte("abcdefghij")
	_ = fs.WriteChunk(fileID, 0, content)
	_ = fs.FinalizeUpload(fileID)

	reader, _, err := fs.OpenFile(fileID, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	data, _ := io.ReadAll(reader)
	if !bytes.Equal(data, []byte("fghij")) {
		t.Errorf("got %q, want %q", data, "fghij")
	}
}

func TestFilesystemTruncate(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFilesystem(dir)
	if err != nil {
		t.Fatal(err)
	}

	fileID, _ := fs.CreateUpload("trunc.txt")
	_ = fs.WriteChunk(fileID, 0, []byte("0123456789"))

	if err := fs.TruncateTo(fileID, 5); err != nil {
		t.Fatal(err)
	}

	size, _ := fs.CommittedSize(fileID)
	if size != 5 {
		t.Errorf("size after truncate: got %d, want 5", size)
	}
}

func TestFilesystemFileHash(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFilesystem(dir)
	if err != nil {
		t.Fatal(err)
	}

	fileID, _ := fs.CreateUpload("hash.txt")
	content := []byte("hash me please")
	_ = fs.WriteChunk(fileID, 0, content)
	_ = fs.FinalizeUpload(fileID)

	hash, err := fs.FileHash(fileID)
	if err != nil {
		t.Fatal(err)
	}

	expected := sha256.Sum256(content)
	if !bytes.Equal(hash, expected[:]) {
		t.Errorf("hash mismatch")
	}
}

func TestFilesystemDelete(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFilesystem(dir)
	if err != nil {
		t.Fatal(err)
	}

	fileID, _ := fs.CreateUpload("delete.txt")
	_ = fs.WriteChunk(fileID, 0, []byte("data"))
	_ = fs.FinalizeUpload(fileID)

	if err := fs.Delete(fileID); err != nil {
		t.Fatal(err)
	}

	_, _, err = fs.OpenFile(fileID, 0)
	if err == nil {
		t.Error("expected error after delete")
	}
}
