package session

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestValidSessionID(t *testing.T) {
	if !ValidSessionID(NewSessionID()) {
		t.Error("generated session ID rejected")
	}
	for _, bad := range []string{"", "..", "../x", "a/b", "nope"} {
		if ValidSessionID(bad) {
			t.Errorf("invalid session ID %q accepted", bad)
		}
	}
}

// A traversal session ID cannot read or write a session file outside the
// store, and loading one looks like a missing session.
func TestSessionStoreRejectsTraversalID(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(filepath.Join(root, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	bad := "../../../pwned"

	if err := store.SaveDownload(&DownloadSession{SessionID: bad}); !errors.Is(err, ErrInvalidSessionID) {
		t.Errorf("SaveDownload: %v, want ErrInvalidSessionID", err)
	}
	if err := store.SaveUpload(&UploadSession{SessionID: bad}); !errors.Is(err, ErrInvalidSessionID) {
		t.Errorf("SaveUpload: %v, want ErrInvalidSessionID", err)
	}
	if _, err := store.LoadUpload(bad); !errors.Is(err, ErrSessionNotFound) {
		t.Errorf("LoadUpload: %v, want ErrSessionNotFound", err)
	}
	// Nothing escaped the store directory.
	if _, err := os.Stat(filepath.Join(root, "pwned.session")); err == nil {
		t.Fatal("a session file was written outside the store")
	}
}
