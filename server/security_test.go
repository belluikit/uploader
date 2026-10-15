package server_test

import (
	"context"
	"crypto/sha256"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

// setupAt builds a server whose storage and sessions live under dir, so a test
// can place files as siblings of the data directory and attempt to reach them.
func setupAt(t *testing.T, dir string) (uploaderpb.TransferServiceClient, func()) {
	t.Helper()
	store, err := storage.NewFilesystem(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := session.NewStore(filepath.Join(dir, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	uploaderpb.RegisterTransferServiceServer(gs, server.New(store, sessions))
	lis := bufconn.Listen(1 << 20)
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
	return uploaderpb.NewTransferServiceClient(conn), func() {
		conn.Close()
		gs.Stop()
		lis.Close()
	}
}

// drainDownload collects a Download stream into bytes, or returns the status.
func drainDownload(t *testing.T, client uploaderpb.TransferServiceClient, req *uploaderpb.DownloadRequest) ([]byte, error) {
	t.Helper()
	stream, err := client.Download(context.Background(), req)
	if err != nil {
		return nil, err
	}
	var got []byte
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return got, nil
		}
		if err != nil {
			return got, err
		}
		if c := msg.GetChunk(); c != nil {
			got = append(got, c.Data...)
		}
	}
}

// A file_id that tries to climb out of the files directory must be refused
// before it reaches the filesystem, and must not read the sibling secret.
func TestDownloadRejectsPathTraversal(t *testing.T) {
	dir := t.TempDir()
	secret := []byte("TOP-SECRET-OUTSIDE-DATA-DIR")
	// store lives at dir/store/{files,staging}; this sits two levels up.
	if err := os.WriteFile(filepath.Join(dir, "secret.txt"), secret, 0o600); err != nil {
		t.Fatal(err)
	}
	client, cleanup := setupAt(t, dir)
	defer cleanup()

	for _, id := range []string{
		"../../secret.txt",
		"../../../etc/hosts",
		"..",
		"a/b",
		"ABCDEF",                // too short / wrong form
		strings.Repeat("g", 32), // right length, not hex
	} {
		got, err := drainDownload(t, client, &uploaderpb.DownloadRequest{FileId: id})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("Download(%q) code = %v, want InvalidArgument", id, status.Code(err))
		}
		if len(got) != 0 {
			t.Errorf("Download(%q) returned %d bytes; traversal leaked data", id, len(got))
		}
	}
}

// A session_id that tries to climb out must be refused before it writes a
// session file anywhere.
func TestDownloadRejectsTraversalSessionID(t *testing.T) {
	dir := t.TempDir()
	client, cleanup := setupAt(t, dir)
	defer cleanup()

	// Upload a real file so a valid file_id exists; only the session_id is bad.
	content := []byte("hello")
	up := mustUpload(t, client, content)

	_, err := drainDownload(t, client, &uploaderpb.DownloadRequest{
		FileId:    up.FileId,
		SessionId: "../../../../tmp/evil",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Download with traversal session_id: code = %v, want InvalidArgument", status.Code(err))
	}
	// No .session file escaped into the temp dir root.
	if _, err := os.Stat(filepath.Join(dir, "..", "tmp", "evil.session")); err == nil {
		t.Fatal("a session file was written outside the sessions directory")
	}
}

// A traversal session_id on the Query and Upload-resume paths is reported as a
// missing session, never as a filesystem escape.
func TestQueryAndResumeRejectTraversalSessionID(t *testing.T) {
	dir := t.TempDir()
	client, cleanup := setupAt(t, dir)
	defer cleanup()

	bad := "../../../../etc/passwd"
	if _, err := client.QueryUpload(context.Background(), &uploaderpb.QueryUploadRequest{SessionId: bad}); status.Code(err) != codes.NotFound {
		t.Errorf("QueryUpload(%q) = %v, want NotFound", bad, status.Code(err))
	}
	if _, err := client.QueryDownload(context.Background(), &uploaderpb.QueryDownloadRequest{SessionId: bad}); status.Code(err) != codes.NotFound {
		t.Errorf("QueryDownload(%q) = %v, want NotFound", bad, status.Code(err))
	}
}

// A download of a well-formed but unknown file_id returns NotFound with a
// generic message, not the server's filesystem path (uploader#8).
func TestDownloadMissingFileHasNoPathInError(t *testing.T) {
	dir := t.TempDir()
	client, cleanup := setupAt(t, dir)
	defer cleanup()

	missing := "00112233445566778899aabbccddeeff"
	_, err := drainDownload(t, client, &uploaderpb.DownloadRequest{FileId: missing})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}
	if msg := status.Convert(err).Message(); strings.Contains(msg, dir) || strings.Contains(msg, "/") {
		t.Errorf("error message %q leaks a server path", msg)
	}
}

// A normal upload then download still works, so the harness is not vacuously
// rejecting everything.
func TestDownloadLegitimateFileWorks(t *testing.T) {
	dir := t.TempDir()
	client, cleanup := setupAt(t, dir)
	defer cleanup()

	content := []byte("the real payload")
	up := mustUpload(t, client, content)
	got, err := drainDownload(t, client, &uploaderpb.DownloadRequest{FileId: up.FileId})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(content) {
		t.Fatalf("download = %q, want %q", got, content)
	}
}

func mustUpload(t *testing.T, client uploaderpb.TransferServiceClient, content []byte) *uploaderpb.UploadResponse {
	t.Helper()
	hash := sha256.Sum256(content)
	stream, err := client.Upload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&uploaderpb.UploadRequest{Payload: &uploaderpb.UploadRequest_Metadata{
		Metadata: &uploaderpb.UploadMetadata{Filename: "f", FileSize: int64(len(content)), FileHash: hash[:]},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&uploaderpb.UploadRequest{Payload: &uploaderpb.UploadRequest_Chunk{
		Chunk: &uploaderpb.UploadChunk{Offset: 0, Data: content},
	}}); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
