// Package server implements the TransferService gRPC server.
package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	uploaderpb "github.com/belluikit/uploader/proto/uploader/v1"
	"github.com/belluikit/uploader/session"
	"github.com/belluikit/uploader/storage"
)

const defaultDownloadChunkSize = 256 * 1024 // 256 KB

// Server implements the TransferService gRPC server.
type Server struct {
	uploaderpb.UnimplementedTransferServiceServer

	storage  storage.Backend
	sessions *session.Store
}

// New creates a new gRPC server backed by the given storage and session store.
func New(storage storage.Backend, sessions *session.Store) *Server {
	return &Server{
		storage:  storage,
		sessions: sessions,
	}
}

// StartSweeper runs a background goroutine that periodically cleans up expired sessions.
// It stops when ctx is cancelled.
func (s *Server) StartSweeper(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				removed, err := s.sessions.Sweep()
				if err != nil {
					slog.Error("session sweep failed", "error", err)
				} else if removed > 0 {
					slog.Info("expired sessions cleaned", "count", removed)
				}
			}
		}
	}()
}

// Upload handles a client-streaming upload of file chunks.
func (s *Server) Upload(stream uploaderpb.TransferService_UploadServer) error {
	// First message must be metadata.
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "recv metadata: %v", err)
	}
	meta := first.GetMetadata()
	if meta == nil {
		return status.Error(codes.InvalidArgument, "first message must contain metadata")
	}
	if err := validateUploadMetadata(meta); err != nil {
		return err
	}

	sess, err := s.resolveUploadSession(meta)
	if err != nil {
		return err
	}

	// Receive chunks.
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Save progress before returning error.
			_ = s.sessions.SaveUpload(sess)
			return status.Errorf(codes.Internal, "recv chunk: %v", err)
		}
		chunk := msg.GetChunk()
		if chunk == nil {
			return status.Error(codes.InvalidArgument, "expected chunk message")
		}
		if err := s.storage.WriteChunk(sess.FileID, chunk.Offset, chunk.Data); err != nil {
			return status.Errorf(codes.Internal, "write chunk: %v", err)
		}
		sess.CommittedOffset = chunk.Offset + int64(len(chunk.Data))
		if err := s.sessions.SaveUpload(sess); err != nil {
			return status.Errorf(codes.Internal, "save session: %v", err)
		}
	}

	// Check if the full file has been received. A session that was already
	// complete is finalized once only: this is what a client sees when it
	// resumes after the response to a finished upload was lost.
	complete := sess.CommittedOffset >= sess.FileSize
	if complete && !sess.Complete {
		if err := s.storage.FinalizeUpload(sess.FileID); err != nil {
			return status.Errorf(codes.Internal, "finalize upload: %v", err)
		}
		sess.Complete = true
		_ = s.sessions.SaveUpload(sess)
	}

	return stream.SendAndClose(&uploaderpb.UploadResponse{
		SessionId:     sess.SessionID,
		FileId:        sess.FileID,
		CommittedSize: sess.CommittedOffset,
		Complete:      complete,
	})
}

// StartUpload creates an upload session, or validates and reports an existing
// one, before any data is sent, so the client knows the session ID up front.
func (s *Server) StartUpload(_ context.Context, req *uploaderpb.StartUploadRequest) (*uploaderpb.StartUploadResponse, error) {
	meta := req.GetMetadata()
	if meta == nil {
		return nil, status.Error(codes.InvalidArgument, "metadata is required")
	}
	if err := validateUploadMetadata(meta); err != nil {
		return nil, err
	}
	// An existing session is only reported, never rolled back: unlike Upload,
	// StartUpload sends no data, so resume_offset carries no intent to rewind.
	var sess *session.UploadSession
	var err error
	if meta.SessionId != "" {
		sess, err = s.loadUploadSession(meta)
	} else {
		sess, err = s.resolveUploadSession(meta)
	}
	if err != nil {
		return nil, err
	}
	return &uploaderpb.StartUploadResponse{
		SessionId:       sess.SessionID,
		CommittedOffset: sess.CommittedOffset,
	}, nil
}

// validateUploadMetadata checks the fields every upload must carry.
func validateUploadMetadata(meta *uploaderpb.UploadMetadata) error {
	if meta.Filename == "" {
		return status.Error(codes.InvalidArgument, "filename is required")
	}
	if meta.FileSize <= 0 {
		return status.Error(codes.InvalidArgument, "file_size must be positive")
	}
	if len(meta.FileHash) == 0 {
		return status.Error(codes.InvalidArgument, "file_hash is required")
	}
	return nil
}

// resolveUploadSession returns an existing session (for resume) or creates a new one.
func (s *Server) resolveUploadSession(meta *uploaderpb.UploadMetadata) (*session.UploadSession, error) {
	if meta.SessionId != "" {
		// Resume existing session.
		sess, err := s.loadUploadSession(meta)
		if err != nil {
			return nil, err
		}
		// A finished upload has been moved out of staging; it cannot be
		// rewound, only reported.
		if sess.Complete && meta.ResumeOffset < sess.CommittedOffset {
			return nil, status.Error(codes.FailedPrecondition, "upload already complete")
		}
		// If client wants to resume at an earlier offset, truncate.
		if meta.ResumeOffset < sess.CommittedOffset {
			if err := s.storage.TruncateTo(sess.FileID, meta.ResumeOffset); err != nil {
				return nil, status.Errorf(codes.Internal, "truncate: %v", err)
			}
			sess.CommittedOffset = meta.ResumeOffset
		}
		return sess, nil
	}

	// New upload session.
	fileID, err := s.storage.CreateUpload(meta.Filename)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create upload: %v", err)
	}
	now := time.Now().Unix()
	sess := &session.UploadSession{
		SessionID: session.NewSessionID(),
		Filename:  meta.Filename,
		FileSize:  meta.FileSize,
		FileHash:  meta.FileHash,
		FileID:    fileID,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := s.sessions.SaveUpload(sess); err != nil {
		return nil, status.Errorf(codes.Internal, "save new session: %v", err)
	}
	return sess, nil
}

// loadUploadSession loads the session named in meta and checks that it is for
// the same file.
func (s *Server) loadUploadSession(meta *uploaderpb.UploadMetadata) (*session.UploadSession, error) {
	sess, err := s.sessions.LoadUpload(meta.SessionId)
	if err != nil {
		if err == session.ErrSessionExpired {
			return nil, status.Error(codes.FailedPrecondition, "session expired")
		}
		if err == session.ErrSessionNotFound {
			return nil, status.Error(codes.NotFound, "upload session not found")
		}
		return nil, status.Errorf(codes.Internal, "load session: %v", err)
	}
	if !bytes.Equal(sess.FileHash, meta.FileHash) {
		return nil, status.Error(codes.FailedPrecondition, "file changed since session was created")
	}
	return sess, nil
}

// Download handles a server-streaming download of file chunks.
func (s *Server) Download(req *uploaderpb.DownloadRequest, stream uploaderpb.TransferService_DownloadServer) error {
	if req.FileId == "" {
		return status.Error(codes.InvalidArgument, "file_id is required")
	}

	// Check file integrity on resume.
	if len(req.FileHash) > 0 {
		currentHash, err := s.storage.FileHash(req.FileId)
		if err != nil {
			return status.Errorf(codes.Internal, "compute file hash: %v", err)
		}
		if !bytes.Equal(currentHash, req.FileHash) {
			return status.Error(codes.FailedPrecondition, "file changed since download session was created")
		}
	}

	reader, totalSize, err := s.storage.OpenFile(req.FileId, req.Offset)
	if err != nil {
		return status.Errorf(codes.NotFound, "open file: %v", err)
	}
	defer reader.Close()

	// Compute hash for metadata (only for new downloads without offset).
	var fileHash []byte
	if req.Offset == 0 {
		h, err := s.storage.FileHash(req.FileId)
		if err != nil {
			return status.Errorf(codes.Internal, "file hash: %v", err)
		}
		fileHash = h
	} else {
		fileHash = req.FileHash
	}

	// Create or resume download session.
	sessID := req.SessionId
	if sessID == "" {
		sessID = session.NewSessionID()
	}
	sess := &session.DownloadSession{
		SessionID:        sessID,
		FileID:           req.FileId,
		FileSize:         totalSize,
		FileHash:         fileHash,
		DownloadedOffset: req.Offset,
	}
	if err := s.sessions.SaveDownload(sess); err != nil {
		return status.Errorf(codes.Internal, "save download session: %v", err)
	}

	// Send metadata first.
	if err := stream.Send(&uploaderpb.DownloadResponse{
		Payload: &uploaderpb.DownloadResponse_Metadata{
			Metadata: &uploaderpb.DownloadMetadata{
				SessionId: sessID,
				FileSize:  totalSize,
				FileHash:  fileHash,
			},
		},
	}); err != nil {
		return status.Errorf(codes.Internal, "send metadata: %v", err)
	}

	// Stream chunks.
	buf := make([]byte, defaultDownloadChunkSize)
	offset := req.Offset
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			if err := stream.Send(&uploaderpb.DownloadResponse{
				Payload: &uploaderpb.DownloadResponse_Chunk{
					Chunk: &uploaderpb.DownloadChunk{
						Offset: offset,
						Data:   buf[:n],
					},
				},
			}); err != nil {
				return status.Errorf(codes.Internal, "send chunk: %v", err)
			}
			offset += int64(n)
			sess.DownloadedOffset = offset
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			_ = s.sessions.SaveDownload(sess)
			return status.Errorf(codes.Internal, "read file: %v", readErr)
		}
	}

	sess.Complete = true
	_ = s.sessions.SaveDownload(sess)
	return nil
}

// QueryUpload returns the current state of an upload session.
func (s *Server) QueryUpload(_ context.Context, req *uploaderpb.QueryUploadRequest) (*uploaderpb.QueryUploadResponse, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	sess, err := s.sessions.LoadUpload(req.SessionId)
	if err != nil {
		if err == session.ErrSessionExpired {
			return &uploaderpb.QueryUploadResponse{
				SessionId: req.SessionId,
				Expired:   true,
			}, nil
		}
		if err == session.ErrSessionNotFound {
			return nil, status.Error(codes.NotFound, "upload session not found")
		}
		return nil, status.Errorf(codes.Internal, "load session: %v", err)
	}
	return &uploaderpb.QueryUploadResponse{
		SessionId:       sess.SessionID,
		CommittedOffset: sess.CommittedOffset,
		FileHash:        sess.FileHash,
		Expired:         false,
	}, nil
}

// QueryDownload returns the current state of a download session.
func (s *Server) QueryDownload(_ context.Context, req *uploaderpb.QueryDownloadRequest) (*uploaderpb.QueryDownloadResponse, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	sess, err := s.sessions.LoadDownload(req.SessionId)
	if err != nil {
		if err == session.ErrSessionExpired {
			return &uploaderpb.QueryDownloadResponse{
				SessionId: req.SessionId,
				Expired:   true,
			}, nil
		}
		if err == session.ErrSessionNotFound {
			return nil, status.Error(codes.NotFound, "download session not found")
		}
		return nil, status.Errorf(codes.Internal, "load session: %v", err)
	}
	return &uploaderpb.QueryDownloadResponse{
		SessionId:        sess.SessionID,
		FileId:           sess.FileID,
		DownloadedOffset: sess.DownloadedOffset,
		FileHash:         sess.FileHash,
		Expired:          false,
	}, nil
}

// compile-time check
var _ uploaderpb.TransferServiceServer = (*Server)(nil)
