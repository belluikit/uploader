package sdk

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	uploaderpb "github.com/belluikit/uploader/proto/uploader/v1"
)

// Upload sends a local file to the transfer service.
// It computes a SHA-256 hash for integrity checking, supports resumable
// uploads via WithSessionID, and reports progress via WithProgress.
//
// The session is created before any data is sent, so the returned
// UploadResult carries a SessionID even when the upload fails part-way; pass
// it to WithSessionID to resume. Transient failures (the service unavailable,
// a stream reset) are resumed automatically up to WithMaxRetries times.
func (c *Client) Upload(ctx context.Context, filePath string, opts ...Option) (*UploadResult, error) {
	o := defaultOptions()
	for _, fn := range opts {
		fn(&o)
	}

	// Compute file hash.
	fileHash, fileSize, err := hashFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("hash file: %w", err)
	}

	sessionID, resumeOffset, err := c.startUpload(ctx, o.sessionID, &uploaderpb.UploadMetadata{
		Filename: filePath,
		FileSize: fileSize,
		FileHash: fileHash,
	})
	if err != nil {
		return nil, err
	}

	for attempt := 0; ; attempt++ {
		res, err := c.doUpload(ctx, filePath, fileHash, fileSize, sessionID, resumeOffset, &o)
		// Without a session ID (a server that predates StartUpload) there is
		// nothing to resume from, so a failure is final.
		if err == nil || sessionID == "" || attempt >= o.maxRetries || !isRetryable(ctx, err) {
			return res, err
		}
		if sleepCtx(ctx, retryBackoff(attempt)) != nil {
			return res, err
		}
		// Resume from what the server committed. If the query itself fails
		// transiently, keep the previous offset: resuming earlier than the
		// committed offset is safe, the server rewinds to it.
		q, qerr := c.rpc.QueryUpload(ctx, &uploaderpb.QueryUploadRequest{SessionId: sessionID})
		switch {
		case qerr == nil && q.Expired:
			return res, err
		case qerr == nil:
			resumeOffset = q.CommittedOffset
		case !isRetryable(ctx, qerr):
			return res, err
		}
	}
}

// startUpload returns the session to upload into and the offset to start
// from. A requested session is resumed when it still exists for the same
// file; otherwise a new session is created with StartUpload. A server without
// StartUpload yields an empty session ID and the upload proceeds as before.
func (c *Client) startUpload(ctx context.Context, requested string, meta *uploaderpb.UploadMetadata) (string, int64, error) {
	if requested != "" {
		resp, err := c.rpc.QueryUpload(ctx, &uploaderpb.QueryUploadRequest{
			SessionId: requested,
		})
		switch {
		case err != nil && status.Code(err) != codes.NotFound:
			return "", 0, fmt.Errorf("query upload session: %w", err)
		case err == nil && !resp.Expired && hashEqual(resp.FileHash, meta.FileHash):
			return requested, resp.CommittedOffset, nil
		}
		// Session gone, expired, or for a different file: start fresh.
	}

	resp, err := c.rpc.StartUpload(ctx, &uploaderpb.StartUploadRequest{Metadata: meta})
	if status.Code(err) == codes.Unimplemented {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, fmt.Errorf("start upload session: %w", err)
	}
	return resp.SessionId, resp.CommittedOffset, nil
}

func (c *Client) doUpload(ctx context.Context, filePath string, fileHash []byte, fileSize int64, sessionID string, resumeOffset int64, o *options) (*UploadResult, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	if resumeOffset > 0 {
		if _, err := f.Seek(resumeOffset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek to resume offset: %w", err)
		}
	}

	stream, err := c.rpc.Upload(ctx)
	if err != nil {
		return nil, fmt.Errorf("open upload stream: %w", err)
	}

	// Send metadata.
	if err := stream.Send(&uploaderpb.UploadRequest{
		Payload: &uploaderpb.UploadRequest_Metadata{
			Metadata: &uploaderpb.UploadMetadata{
				SessionId:    sessionID,
				Filename:     filePath,
				FileSize:     fileSize,
				FileHash:     fileHash,
				ResumeOffset: resumeOffset,
			},
		},
	}); err != nil {
		return &UploadResult{SessionID: sessionID, CommittedSize: resumeOffset},
			fmt.Errorf("send metadata: %w", streamError(stream, err))
	}

	// Stream chunks.
	buf := make([]byte, o.chunkSize)
	offset := resumeOffset
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if err := stream.Send(&uploaderpb.UploadRequest{
				Payload: &uploaderpb.UploadRequest_Chunk{
					Chunk: &uploaderpb.UploadChunk{
						Offset: offset,
						Data:   buf[:n],
					},
				},
			}); err != nil {
				return &UploadResult{
					SessionID:     sessionID,
					CommittedSize: offset,
				}, fmt.Errorf("send chunk at offset %d: %w", offset, streamError(stream, err))
			}
			offset += int64(n)
			if o.progress != nil {
				o.progress(offset, fileSize)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return &UploadResult{
				SessionID:     sessionID,
				CommittedSize: offset,
			}, fmt.Errorf("read file: %w", readErr)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return &UploadResult{
			SessionID:     sessionID,
			CommittedSize: offset,
		}, fmt.Errorf("close upload stream: %w", err)
	}

	return &UploadResult{
		SessionID:     resp.SessionId,
		FileID:        resp.FileId,
		CommittedSize: resp.CommittedSize,
		Complete:      resp.Complete,
	}, nil
}

// streamError returns the status that ended an upload stream. When the server
// ends the stream, Send reports only io.EOF; the actual error, and with it
// whether the failure is retryable, comes from CloseAndRecv.
func streamError(stream uploaderpb.TransferService_UploadClient, err error) error {
	if err == io.EOF {
		if _, rerr := stream.CloseAndRecv(); rerr != nil {
			return rerr
		}
	}
	return err
}

// hashFile computes the SHA-256 hash and size of a file.
func hashFile(path string) ([]byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return nil, 0, err
	}
	return h.Sum(nil), size, nil
}

func hashEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
