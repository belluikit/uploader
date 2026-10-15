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

	// Determine resume offset.
	sessionID := o.sessionID
	var resumeOffset int64

	if sessionID != "" {
		resp, err := c.rpc.QueryUpload(ctx, &uploaderpb.QueryUploadRequest{
			SessionId: sessionID,
		})
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.NotFound {
				// Session gone, start fresh.
				sessionID = ""
			} else {
				return nil, fmt.Errorf("query upload session: %w", err)
			}
		} else if resp.Expired {
			sessionID = ""
		} else {
			// Validate file integrity.
			if !hashEqual(resp.FileHash, fileHash) {
				// File changed — restart.
				sessionID = ""
			} else {
				resumeOffset = resp.CommittedOffset
			}
		}
	}

	return c.doUpload(ctx, filePath, fileHash, fileSize, sessionID, resumeOffset, &o)
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
		return nil, fmt.Errorf("send metadata: %w", err)
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
				}, fmt.Errorf("send chunk at offset %d: %w", offset, err)
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
