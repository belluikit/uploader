package session

import (
	"testing"
	"time"
)

func TestUploadSessionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	sess := &UploadSession{
		SessionID:       "11111111111111111111111111111111",
		Filename:        "hello.txt",
		FileSize:        1024,
		FileHash:        []byte{0xde, 0xad, 0xbe, 0xef},
		CommittedOffset: 512,
		FileID:          "file-abc",
		CreatedAt:       time.Now().Unix(),
	}

	if err := store.SaveUpload(sess); err != nil {
		t.Fatal("save:", err)
	}

	loaded, err := store.LoadUpload("11111111111111111111111111111111")
	if err != nil {
		t.Fatal("load:", err)
	}

	if loaded.SessionID != sess.SessionID {
		t.Errorf("SessionID: got %q, want %q", loaded.SessionID, sess.SessionID)
	}
	if loaded.Filename != sess.Filename {
		t.Errorf("Filename: got %q, want %q", loaded.Filename, sess.Filename)
	}
	if loaded.FileSize != sess.FileSize {
		t.Errorf("FileSize: got %d, want %d", loaded.FileSize, sess.FileSize)
	}
	if loaded.CommittedOffset != sess.CommittedOffset {
		t.Errorf("CommittedOffset: got %d, want %d", loaded.CommittedOffset, sess.CommittedOffset)
	}
	if loaded.FileID != sess.FileID {
		t.Errorf("FileID: got %q, want %q", loaded.FileID, sess.FileID)
	}
	if len(loaded.FileHash) != len(sess.FileHash) {
		t.Errorf("FileHash length: got %d, want %d", len(loaded.FileHash), len(sess.FileHash))
	}
}

func TestDownloadSessionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	sess := &DownloadSession{
		SessionID:        "22222222222222222222222222222222",
		FileID:           "file-xyz",
		Filename:         "data.bin",
		FileSize:         2048,
		FileHash:         []byte{0xca, 0xfe},
		DownloadedOffset: 1024,
		CreatedAt:        time.Now().Unix(),
	}

	if err := store.SaveDownload(sess); err != nil {
		t.Fatal("save:", err)
	}

	loaded, err := store.LoadDownload("22222222222222222222222222222222")
	if err != nil {
		t.Fatal("load:", err)
	}

	if loaded.SessionID != sess.SessionID {
		t.Errorf("SessionID: got %q, want %q", loaded.SessionID, sess.SessionID)
	}
	if loaded.FileID != sess.FileID {
		t.Errorf("FileID: got %q, want %q", loaded.FileID, sess.FileID)
	}
	if loaded.DownloadedOffset != sess.DownloadedOffset {
		t.Errorf("DownloadedOffset: got %d, want %d", loaded.DownloadedOffset, sess.DownloadedOffset)
	}
}

func TestSessionExpiry(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.SetTTL(1 * time.Second)

	sess := &UploadSession{
		SessionID: "33333333333333333333333333333333",
		Filename:  "old.txt",
		FileSize:  100,
		FileHash:  []byte{1},
		FileID:    "f1",
		CreatedAt: time.Now().Unix(),
	}

	if err := store.SaveUpload(sess); err != nil {
		t.Fatal(err)
	}

	// Should load fine immediately.
	if _, err := store.LoadUpload("33333333333333333333333333333333"); err != nil {
		t.Fatal("expected success, got:", err)
	}

	// Wait for expiry.
	time.Sleep(2 * time.Second)

	_, err = store.LoadUpload("33333333333333333333333333333333")
	if err != ErrSessionExpired {
		t.Errorf("expected ErrSessionExpired, got: %v", err)
	}
}

func TestSessionNotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.LoadUpload("nonexistent")
	if err != ErrSessionNotFound {
		t.Errorf("expected ErrSessionNotFound, got: %v", err)
	}

	_, err = store.LoadDownload("nonexistent")
	if err != ErrSessionNotFound {
		t.Errorf("expected ErrSessionNotFound, got: %v", err)
	}
}

func TestSweep(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.SetTTL(1 * time.Second)

	for i := range 5 {
		sess := &UploadSession{
			SessionID: NewSessionID(),
			Filename:  "f.txt",
			FileSize:  int64(i + 1),
			FileHash:  []byte{byte(i)},
			FileID:    NewSessionID(),
			CreatedAt: time.Now().Unix(),
		}
		if err := store.SaveUpload(sess); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(2 * time.Second)

	removed, err := store.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 5 {
		t.Errorf("expected 5 removed, got %d", removed)
	}
}
