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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	uploaderpb "github.com/belluikit/uploader/proto/uploader/v1"
	"github.com/belluikit/uploader/server"
	"github.com/belluikit/uploader/session"
	"github.com/belluikit/uploader/storage"
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

func TestStartUploadThenUpload(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	ctx := context.Background()
	content := bytes.Repeat([]byte("0123456789"), 300) // 3000 bytes
	hash := sha256.Sum256(content)
	meta := &uploaderpb.UploadMetadata{
		Filename: "start.bin",
		FileSize: int64(len(content)),
		FileHash: hash[:],
	}

	// A new session exists, with an ID, before any data is sent.
	start, err := client.StartUpload(ctx, &uploaderpb.StartUploadRequest{Metadata: meta})
	if err != nil {
		t.Fatal(err)
	}
	if start.SessionId == "" {
		t.Fatal("expected a session ID from StartUpload")
	}
	if start.CommittedOffset != 0 {
		t.Fatalf("new session committed_offset = %d, want 0", start.CommittedOffset)
	}
	q, err := client.QueryUpload(ctx, &uploaderpb.QueryUploadRequest{SessionId: start.SessionId})
	if err != nil {
		t.Fatalf("QueryUpload on a started session: %v", err)
	}
	if !bytes.Equal(q.FileHash, hash[:]) {
		t.Error("started session does not carry the file hash")
	}

	// Send the first half on that session.
	half := len(content) / 2
	stream, err := client.Upload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	withID := &uploaderpb.UploadMetadata{
		SessionId: start.SessionId,
		Filename:  meta.Filename,
		FileSize:  meta.FileSize,
		FileHash:  meta.FileHash,
	}
	if err := stream.Send(&uploaderpb.UploadRequest{Payload: &uploaderpb.UploadRequest_Metadata{Metadata: withID}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&uploaderpb.UploadRequest{Payload: &uploaderpb.UploadRequest_Chunk{
		Chunk: &uploaderpb.UploadChunk{Offset: 0, Data: content[:half]},
	}}); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if resp.SessionId != start.SessionId {
		t.Errorf("Upload used session %q, want the started %q", resp.SessionId, start.SessionId)
	}
	if resp.Complete {
		t.Error("half an upload reported complete")
	}

	// StartUpload on the existing session reports where to resume.
	again, err := client.StartUpload(ctx, &uploaderpb.StartUploadRequest{Metadata: withID})
	if err != nil {
		t.Fatal(err)
	}
	if again.SessionId != start.SessionId || again.CommittedOffset != int64(half) {
		t.Errorf("StartUpload(existing) = (%q, %d), want (%q, %d)",
			again.SessionId, again.CommittedOffset, start.SessionId, half)
	}
}

func TestStartUploadValidation(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	ctx := context.Background()
	hash := sha256.Sum256([]byte("x"))
	cases := map[string]*uploaderpb.StartUploadRequest{
		"no metadata": {},
		"no filename": {Metadata: &uploaderpb.UploadMetadata{FileSize: 1, FileHash: hash[:]}},
		"no size":     {Metadata: &uploaderpb.UploadMetadata{Filename: "f", FileHash: hash[:]}},
		"no hash":     {Metadata: &uploaderpb.UploadMetadata{Filename: "f", FileSize: 1}},
	}
	for name, req := range cases {
		if _, err := client.StartUpload(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v, want InvalidArgument", name, err)
		}
	}
	unknown := &uploaderpb.StartUploadRequest{Metadata: &uploaderpb.UploadMetadata{
		SessionId: "does-not-exist", Filename: "f", FileSize: 1, FileHash: hash[:],
	}}
	if _, err := client.StartUpload(ctx, unknown); status.Code(err) != codes.NotFound {
		t.Errorf("unknown session: got %v, want NotFound", err)
	}
}

func TestUploadOnCompletedSession(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	ctx := context.Background()
	content := bytes.Repeat([]byte("done"), 500)
	hash := sha256.Sum256(content)
	send := func(meta *uploaderpb.UploadMetadata, data []byte) (*uploaderpb.UploadResponse, error) {
		stream, err := client.Upload(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&uploaderpb.UploadRequest{Payload: &uploaderpb.UploadRequest_Metadata{Metadata: meta}}); err != nil {
			t.Fatal(err)
		}
		if len(data) > 0 {
			if err := stream.Send(&uploaderpb.UploadRequest{Payload: &uploaderpb.UploadRequest_Chunk{
				Chunk: &uploaderpb.UploadChunk{Offset: meta.ResumeOffset, Data: data},
			}}); err != nil {
				t.Fatal(err)
			}
		}
		return stream.CloseAndRecv()
	}

	first, err := send(&uploaderpb.UploadMetadata{Filename: "f", FileSize: int64(len(content)), FileHash: hash[:]}, content)
	if err != nil || !first.Complete {
		t.Fatalf("first upload: %+v %v", first, err)
	}

	// Resuming at the end of a completed session reports it again.
	again, err := send(&uploaderpb.UploadMetadata{
		SessionId: first.SessionId, Filename: "f", FileSize: int64(len(content)),
		FileHash: hash[:], ResumeOffset: int64(len(content)),
	}, nil)
	if err != nil {
		t.Fatalf("resume of a completed session: %v", err)
	}
	if !again.Complete || again.FileId != first.FileId {
		t.Errorf("resume of a completed session = %+v, want complete with file %s", again, first.FileId)
	}

	// Rewinding a finalized upload is refused, not attempted.
	_, err = send(&uploaderpb.UploadMetadata{
		SessionId: first.SessionId, Filename: "f", FileSize: int64(len(content)),
		FileHash: hash[:], ResumeOffset: 0,
	}, content[:10])
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("rewind of a completed session: %v, want FailedPrecondition", err)
	}
}
