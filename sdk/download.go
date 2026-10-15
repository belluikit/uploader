package sdk

import (
	"context"
	"fmt"
	"io"
	"os"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	uploaderpb "github.com/belluikit/uploader/proto/uploader/v1"
)

// Download retrieves a file from the transfer service and writes it to destPath.
// It supports resumable downloads via WithSessionID and reports progress via WithProgress.
func (c *Client) Download(ctx context.Context, fileID, destPath string, opts ...Option) error {
	o := defaultOptions()
	for _, fn := range opts {
		fn(&o)
	}

	sessionID := o.sessionID
	var offset int64
	var expectedHash []byte

	// Check for partial download on disk and session state.
	if sessionID != "" {
		resp, err := c.rpc.QueryDownload(ctx, &uploaderpb.QueryDownloadRequest{
			SessionId: sessionID,
		})
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.NotFound {
				sessionID = ""
			} else {
				return fmt.Errorf("query download session: %w", err)
			}
		} else if resp.Expired {
			sessionID = ""
		} else {
			expectedHash = resp.FileHash
			// Use the local file size as the resume offset.
			if info, err := os.Stat(destPath); err == nil {
				offset = info.Size()
			}
		}
	}

	return c.doDownload(ctx, fileID, destPath, sessionID, offset, expectedHash, &o)
}

func (c *Client) doDownload(ctx context.Context, fileID, destPath, sessionID string, offset int64, expectedHash []byte, o *options) error {
	stream, err := c.rpc.Download(ctx, &uploaderpb.DownloadRequest{
		FileId:    fileID,
		SessionId: sessionID,
		Offset:    offset,
		FileHash:  expectedHash,
	})
	if err != nil {
		return fmt.Errorf("open download stream: %w", err)
	}

	// First message must be metadata.
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("recv metadata: %w", err)
	}
	meta := first.GetMetadata()
	if meta == nil {
		return fmt.Errorf("expected metadata as first message")
	}

	totalSize := meta.FileSize

	// Open destination file for writing.
	var f *os.File
	if offset > 0 {
		f, err = os.OpenFile(destPath, os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("open dest for resume: %w", err)
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return fmt.Errorf("seek dest: %w", err)
		}
	} else {
		f, err = os.Create(destPath)
		if err != nil {
			return fmt.Errorf("create dest: %w", err)
		}
	}
	defer f.Close()

	// Receive chunks.
	current := offset
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("recv chunk: %w", err)
		}
		chunk := msg.GetChunk()
		if chunk == nil {
			return fmt.Errorf("expected chunk message")
		}
		if _, err := f.Write(chunk.Data); err != nil {
			return fmt.Errorf("write chunk: %w", err)
		}
		current += int64(len(chunk.Data))
		if o.progress != nil {
			o.progress(current, totalSize)
		}
	}

	return nil
}
