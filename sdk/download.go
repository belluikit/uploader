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

// DownloadResult describes a completed or partial download.
type DownloadResult struct {
	// SessionID identifies the download session; pass it to WithSessionID to
	// resume into the same destination file.
	SessionID string
	// Size is the total size of the file on the server.
	Size int64
	// Complete is true when the whole file has been written to destPath.
	Complete bool
}

// Download retrieves a file from the transfer service and writes it to destPath.
// It supports resumable downloads via WithSessionID and reports progress via WithProgress.
//
// The returned DownloadResult carries the session ID even when the download
// fails part-way. Transient failures are resumed automatically from the bytes
// already in destPath, up to WithMaxRetries times.
func (c *Client) Download(ctx context.Context, fileID, destPath string, opts ...Option) (*DownloadResult, error) {
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
				return nil, fmt.Errorf("query download session: %w", err)
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

	for attempt := 0; ; attempt++ {
		state, err := c.doDownload(ctx, fileID, destPath, sessionID, offset, expectedHash, &o)
		if state.sessionID != "" {
			sessionID = state.sessionID
		}
		if len(state.fileHash) > 0 {
			expectedHash = state.fileHash
		}
		res := &DownloadResult{
			SessionID: sessionID,
			Size:      state.size,
			Complete:  err == nil && state.written >= state.size,
		}
		if err == nil || sessionID == "" || attempt >= o.maxRetries || !isRetryable(ctx, err) {
			return res, err
		}
		if sleepCtx(ctx, retryBackoff(attempt)) != nil {
			return res, err
		}
		// Resume from what actually reached the disk.
		info, serr := os.Stat(destPath)
		if serr != nil {
			return res, err
		}
		offset = info.Size()
	}
}

// downloadState is what one download attempt learned: the session and file
// identity from the metadata, and how many bytes are in the destination.
type downloadState struct {
	sessionID string
	size      int64
	fileHash  []byte
	written   int64
}

func (c *Client) doDownload(ctx context.Context, fileID, destPath, sessionID string, offset int64, expectedHash []byte, o *options) (downloadState, error) {
	state := downloadState{written: offset}
	stream, err := c.rpc.Download(ctx, &uploaderpb.DownloadRequest{
		FileId:    fileID,
		SessionId: sessionID,
		Offset:    offset,
		FileHash:  expectedHash,
	})
	if err != nil {
		return state, fmt.Errorf("open download stream: %w", err)
	}

	// First message must be metadata.
	first, err := stream.Recv()
	if err != nil {
		return state, fmt.Errorf("recv metadata: %w", err)
	}
	meta := first.GetMetadata()
	if meta == nil {
		return state, fmt.Errorf("expected metadata as first message")
	}

	totalSize := meta.FileSize
	state.sessionID = meta.SessionId
	state.size = meta.FileSize
	state.fileHash = meta.FileHash

	// Open destination file for writing.
	var f *os.File
	if offset > 0 {
		f, err = os.OpenFile(destPath, os.O_WRONLY, 0o644)
		if err != nil {
			return state, fmt.Errorf("open dest for resume: %w", err)
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return state, fmt.Errorf("seek dest: %w", err)
		}
	} else {
		f, err = os.Create(destPath)
		if err != nil {
			return state, fmt.Errorf("create dest: %w", err)
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
			return state, fmt.Errorf("recv chunk: %w", err)
		}
		chunk := msg.GetChunk()
		if chunk == nil {
			return state, fmt.Errorf("expected chunk message")
		}
		if _, err := f.Write(chunk.Data); err != nil {
			return state, fmt.Errorf("write chunk: %w", err)
		}
		current += int64(len(chunk.Data))
		state.written = current
		if o.progress != nil {
			o.progress(current, totalSize)
		}
	}

	return state, nil
}
