// Package session provides upload/download session tracking with
// filesystem-backed persistence using protowire binary encoding.
package session

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/trendvidia/protowire/encoding/pb"
)

// ErrSessionExpired is returned when a session has exceeded its TTL.
var ErrSessionExpired = errors.New("session expired")

// ErrSessionNotFound is returned when a session file does not exist.
var ErrSessionNotFound = errors.New("session not found")

// DefaultTTL is the default time-to-live for sessions (24 hours).
const DefaultTTL = 24 * time.Hour

// UploadSession tracks an in-progress or completed upload.
type UploadSession struct {
	SessionID       string `protowire:"1"`
	Filename        string `protowire:"2"`
	FileSize        int64  `protowire:"3"`
	FileHash        []byte `protowire:"4"` // SHA-256
	CommittedOffset int64  `protowire:"5"`
	FileID          string `protowire:"6"`
	CreatedAt       int64  `protowire:"7"` // unix seconds
	UpdatedAt       int64  `protowire:"8"` // unix seconds
	Complete        bool   `protowire:"9"`
}

// IsExpired reports whether the session has exceeded the given TTL.
func (s *UploadSession) IsExpired(ttl time.Duration) bool {
	return time.Now().Unix()-s.UpdatedAt > int64(ttl.Seconds())
}

// DownloadSession tracks an in-progress or completed download.
type DownloadSession struct {
	SessionID        string `protowire:"1"`
	FileID           string `protowire:"2"`
	Filename         string `protowire:"3"`
	FileSize         int64  `protowire:"4"`
	FileHash         []byte `protowire:"5"` // SHA-256
	DownloadedOffset int64  `protowire:"6"`
	CreatedAt        int64  `protowire:"7"`
	UpdatedAt        int64  `protowire:"8"`
	Complete         bool   `protowire:"9"`
}

// IsExpired reports whether the session has exceeded the given TTL.
func (s *DownloadSession) IsExpired(ttl time.Duration) bool {
	return time.Now().Unix()-s.UpdatedAt > int64(ttl.Seconds())
}

// Store persists session state to the filesystem using protowire encoding.
type Store struct {
	dir string
	ttl time.Duration
}

// NewStore creates a session store rooted at dir.
// It creates the upload/ and download/ subdirectories if they don't exist.
func NewStore(dir string) (*Store, error) {
	for _, sub := range []string{"upload", "download"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("create session dir: %w", err)
		}
	}
	return &Store{dir: dir, ttl: DefaultTTL}, nil
}

// SetTTL overrides the default session TTL.
func (s *Store) SetTTL(ttl time.Duration) {
	s.ttl = ttl
}

// NewSessionID generates a random session identifier.
func NewSessionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// SaveUpload persists an upload session atomically.
func (s *Store) SaveUpload(sess *UploadSession) error {
	sess.UpdatedAt = time.Now().Unix()
	data, err := pb.Marshal(sess)
	if err != nil {
		return fmt.Errorf("marshal upload session: %w", err)
	}
	return atomicWrite(s.uploadPath(sess.SessionID), data)
}

// LoadUpload reads an upload session. Returns ErrSessionNotFound if missing,
// ErrSessionExpired if the session exceeded its TTL (and deletes it).
func (s *Store) LoadUpload(sessionID string) (*UploadSession, error) {
	path := s.uploadPath(sessionID)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrSessionNotFound
		}
		return nil, fmt.Errorf("read upload session: %w", err)
	}
	sess := &UploadSession{}
	if err := pb.Unmarshal(data, sess); err != nil {
		return nil, fmt.Errorf("unmarshal upload session: %w", err)
	}
	if sess.IsExpired(s.ttl) {
		_ = os.Remove(path)
		return nil, ErrSessionExpired
	}
	return sess, nil
}

// DeleteUpload removes an upload session file.
func (s *Store) DeleteUpload(sessionID string) error {
	err := os.Remove(s.uploadPath(sessionID))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// SaveDownload persists a download session atomically.
func (s *Store) SaveDownload(sess *DownloadSession) error {
	sess.UpdatedAt = time.Now().Unix()
	data, err := pb.Marshal(sess)
	if err != nil {
		return fmt.Errorf("marshal download session: %w", err)
	}
	return atomicWrite(s.downloadPath(sess.SessionID), data)
}

// LoadDownload reads a download session. Returns ErrSessionNotFound if missing,
// ErrSessionExpired if the session exceeded its TTL (and deletes it).
func (s *Store) LoadDownload(sessionID string) (*DownloadSession, error) {
	path := s.downloadPath(sessionID)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrSessionNotFound
		}
		return nil, fmt.Errorf("read download session: %w", err)
	}
	sess := &DownloadSession{}
	if err := pb.Unmarshal(data, sess); err != nil {
		return nil, fmt.Errorf("unmarshal download session: %w", err)
	}
	if sess.IsExpired(s.ttl) {
		_ = os.Remove(path)
		return nil, ErrSessionExpired
	}
	return sess, nil
}

// DeleteDownload removes a download session file.
func (s *Store) DeleteDownload(sessionID string) error {
	err := os.Remove(s.downloadPath(sessionID))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Sweep walks all session files and removes expired ones.
// It returns the number of sessions removed.
func (s *Store) Sweep() (int, error) {
	removed := 0
	for _, sub := range []string{"upload", "download"} {
		dir := filepath.Join(s.dir, sub)
		entries, err := os.ReadDir(dir)
		if err != nil {
			return removed, fmt.Errorf("read dir %s: %w", dir, err)
		}
		for _, e := range entries {
			if e.IsDir() || filepath.Ext(e.Name()) != ".session" {
				continue
			}
			path := filepath.Join(dir, e.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var updatedAt int64
			if sub == "upload" {
				sess := &UploadSession{}
				if err := pb.Unmarshal(data, sess); err != nil {
					continue
				}
				updatedAt = sess.UpdatedAt
			} else {
				sess := &DownloadSession{}
				if err := pb.Unmarshal(data, sess); err != nil {
					continue
				}
				updatedAt = sess.UpdatedAt
			}
			if time.Now().Unix()-updatedAt > int64(s.ttl.Seconds()) {
				if err := os.Remove(path); err == nil {
					removed++
				}
			}
		}
	}
	return removed, nil
}

func (s *Store) uploadPath(id string) string {
	return filepath.Join(s.dir, "upload", id+".session")
}

func (s *Store) downloadPath(id string) string {
	return filepath.Join(s.dir, "download", id+".session")
}

// atomicWrite writes data to path via a temporary file + rename.
func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}
