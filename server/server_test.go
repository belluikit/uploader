package server_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	uploaderpb "github.com/trendvidia/uploader/proto/uploader/v1"
	"github.com/trendvidia/uploader/server"
	"github.com/trendvidia/uploader/session"
	"github.com/trendvidia/uploader/storage"
)

func setup(t *testing.T) (uploaderpb.TransferServiceClient, func()) {
	t.Helper()
	dir := t.TempDir()

	store, err := storage.NewFilesystem(dir)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := session.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	srv := server.New(store, sessions)
	gs := grpc.NewServer()
	uploaderpb.RegisterTransferServiceServer(gs, srv)

	lis := bufconn.Listen(1024 * 1024)
	go gs.Serve(lis)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}

	client := uploaderpb.NewTransferServiceClient(conn)
	cleanup := func() {
		conn.Close()
		gs.Stop()
		lis.Close()
	}
	return client, cleanup
}

func TestUploadAndDownload(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	ctx := context.Background()
	content := bytes.Repeat([]byte("ABCDEFGH"), 1024) // 8 KB
	hash := sha256.Sum256(content)

	// Upload.
	stream, err := client.Upload(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = stream.Send(&uploaderpb.UploadRequest{
		Payload: &uploaderpb.UploadRequest_Metadata{
			Metadata: &uploaderpb.UploadMetadata{
				Filename: "test.bin",
				FileSize: int64(len(content)),
				FileHash: hash[:],
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	chunkSize := 1024
	for off := 0; off < len(content); off += chunkSize {
		end := off + chunkSize
		if end > len(content) {
			end = len(content)
		}
		err := stream.Send(&uploaderpb.UploadRequest{
			Payload: &uploaderpb.UploadRequest_Chunk{
				Chunk: &uploaderpb.UploadChunk{
					Offset: int64(off),
					Data:   content[off:end],
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Complete {
		t.Error("expected complete=true")
	}
	if resp.FileId == "" {
		t.Error("expected non-empty file_id")
	}

	// Download.
	dlStream, err := client.Download(ctx, &uploaderpb.DownloadRequest{
		FileId: resp.FileId,
	})
	if err != nil {
		t.Fatal(err)
	}

	// First message = metadata.
	first, err := dlStream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	meta := first.GetMetadata()
	if meta == nil {
		t.Fatal("expected metadata")
	}
	if meta.FileSize != int64(len(content)) {
		t.Errorf("download size: got %d, want %d", meta.FileSize, len(content))
	}

	// Collect chunks.
	var downloaded []byte
	for {
		msg, err := dlStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		chunk := msg.GetChunk()
		if chunk == nil {
			t.Fatal("expected chunk")
		}
		downloaded = append(downloaded, chunk.Data...)
	}

	if !bytes.Equal(downloaded, content) {
		t.Error("downloaded content does not match uploaded content")
	}
}

func TestUploadResumeWithMatchingHash(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	ctx := context.Background()
	content := bytes.Repeat([]byte("X"), 4096)
	hash := sha256.Sum256(content)

	// Upload first half.
	stream1, err := client.Upload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = stream1.Send(&uploaderpb.UploadRequest{
		Payload: &uploaderpb.UploadRequest_Metadata{
			Metadata: &uploaderpb.UploadMetadata{
				Filename: "resume.bin",
				FileSize: int64(len(content)),
				FileHash: hash[:],
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	halfSize := len(content) / 2
	err = stream1.Send(&uploaderpb.UploadRequest{
		Payload: &uploaderpb.UploadRequest_Chunk{
			Chunk: &uploaderpb.UploadChunk{
				Offset: 0,
				Data:   content[:halfSize],
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp1, err := stream1.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if resp1.Complete {
		t.Error("should not be complete yet")
	}

	// Query session.
	qResp, err := client.QueryUpload(ctx, &uploaderpb.QueryUploadRequest{
		SessionId: resp1.SessionId,
	})
	if err != nil {
		t.Fatal(err)
	}
	if qResp.CommittedOffset != int64(halfSize) {
		t.Errorf("committed: got %d, want %d", qResp.CommittedOffset, halfSize)
	}

	// Resume upload with second half.
	stream2, err := client.Upload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = stream2.Send(&uploaderpb.UploadRequest{
		Payload: &uploaderpb.UploadRequest_Metadata{
			Metadata: &uploaderpb.UploadMetadata{
				SessionId:    resp1.SessionId,
				Filename:     "resume.bin",
				FileSize:     int64(len(content)),
				FileHash:     hash[:],
				ResumeOffset: int64(halfSize),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = stream2.Send(&uploaderpb.UploadRequest{
		Payload: &uploaderpb.UploadRequest_Chunk{
			Chunk: &uploaderpb.UploadChunk{
				Offset: int64(halfSize),
				Data:   content[halfSize:],
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := stream2.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if !resp2.Complete {
		t.Error("expected complete after resume")
	}
}

func TestUploadResumeWithMismatchedHash(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	ctx := context.Background()
	content := []byte("original content")
	hash := sha256.Sum256(content)

	// Start upload.
	stream1, err := client.Upload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = stream1.Send(&uploaderpb.UploadRequest{
		Payload: &uploaderpb.UploadRequest_Metadata{
			Metadata: &uploaderpb.UploadMetadata{
				Filename: "mismatch.bin",
				FileSize: int64(len(content)),
				FileHash: hash[:],
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp1, err := stream1.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}

	// Try to resume with different hash.
	differentHash := sha256.Sum256([]byte("modified content"))
	stream2, err := client.Upload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = stream2.Send(&uploaderpb.UploadRequest{
		Payload: &uploaderpb.UploadRequest_Metadata{
			Metadata: &uploaderpb.UploadMetadata{
				SessionId:    resp1.SessionId,
				Filename:     "mismatch.bin",
				FileSize:     100,
				FileHash:     differentHash[:],
				ResumeOffset: 0,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// The server should reject this on CloseAndRecv.
	_, err = stream2.CloseAndRecv()
	if err == nil {
		t.Error("expected error for hash mismatch")
	}
}

func TestSessionExpiry(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewFilesystem(dir)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := session.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	sessions.SetTTL(1 * time.Second)

	srv := server.New(store, sessions)
	gs := grpc.NewServer()
	uploaderpb.RegisterTransferServiceServer(gs, srv)

	lis := bufconn.Listen(1024 * 1024)
	go gs.Serve(lis)
	defer func() { gs.Stop(); lis.Close() }()

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := uploaderpb.NewTransferServiceClient(conn)
	ctx := context.Background()

	// Create a session.
	content := []byte("expire test")
	hash := sha256.Sum256(content)
	stream, err := client.Upload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(&uploaderpb.UploadRequest{
		Payload: &uploaderpb.UploadRequest_Metadata{
			Metadata: &uploaderpb.UploadMetadata{
				Filename: "expire.bin",
				FileSize: int64(len(content)),
				FileHash: hash[:],
			},
		},
	})
	resp, _ := stream.CloseAndRecv()

	// Wait for expiry.
	time.Sleep(2 * time.Second)

	qResp, err := client.QueryUpload(ctx, &uploaderpb.QueryUploadRequest{
		SessionId: resp.SessionId,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !qResp.Expired {
		t.Error("expected session to be expired")
	}
}
