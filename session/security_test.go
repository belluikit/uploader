package session

import (
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
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
	// "../../escaped" from sessions/{upload,download}/ would land at
	// root/escaped.session, inside the tree we walk below.
	bad := "../../escaped"

	if err := store.SaveDownload(&DownloadSession{SessionID: bad}); !errors.Is(err, ErrInvalidSessionID) {
		t.Errorf("SaveDownload: %v, want ErrInvalidSessionID", err)
	}
	if err := store.SaveUpload(&UploadSession{SessionID: bad}); !errors.Is(err, ErrInvalidSessionID) {
		t.Errorf("SaveUpload: %v, want ErrInvalidSessionID", err)
	}
	if _, err := store.LoadUpload(bad); !errors.Is(err, ErrSessionNotFound) {
		t.Errorf("LoadUpload: %v, want ErrSessionNotFound", err)
	}

	// No .session file exists anywhere under root outside the store.
	storeDir := filepath.Join(root, "sessions")
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(p) != ".session" {
			return nil
		}
		if rel, _ := filepath.Rel(storeDir, p); strings.HasPrefix(rel, "..") {
			t.Errorf("session file escaped the store: %s", p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
