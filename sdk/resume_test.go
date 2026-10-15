package sdk

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
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

// errInjected is what a faulty stream reports instead of the next message.
var errInjected = errors.New("injected stream failure")

// faultyServer wraps the real server. Each fault counter makes that many
// streams break part-way, after a few messages, with codes.Unavailable,
// which is what a client sees when a connection drops.
type faultyServer struct {
	*server.Server
	uploadFaults    atomic.Int32
	lostResponses   atomic.Int32
	downloadFaults  atomic.Int32
	noStartUpload   bool
	uploadStreams   atomic.Int32
	downloadStreams atomic.Int32
}

const faultAfterMessages = 3

func (f *faultyServer) StartUpload(ctx context.Context, req *uploaderpb.StartUploadRequest) (*uploaderpb.StartUploadResponse, error) {
	if f.noStartUpload {
		return nil, status.Error(codes.Unimplemented, "method StartUpload not implemented")
	}
	return f.Server.StartUpload(ctx, req)
}

func (f *faultyServer) Upload(stream uploaderpb.TransferService_UploadServer) error {
	f.uploadStreams.Add(1)
	if f.lostResponses.Add(-1) >= 0 {
		// The server finishes, and finalizes, the upload; its response never
		// reaches the client.
		_ = f.Server.Upload(&lostResponse{TransferService_UploadServer: stream})
		return status.Error(codes.Unavailable, "injected: response lost")
	}
	f.lostResponses.Store(0)
	if f.uploadFaults.Add(-1) < 0 {
		f.uploadFaults.Store(0)
		return f.Server.Upload(stream)
	}
	// The metadata message plus faultAfterMessages chunks get through.
	_ = f.Server.Upload(&brokenUpload{TransferService_UploadServer: stream, left: 1 + faultAfterMessages})
	return status.Error(codes.Unavailable, "injected: connection lost")
}

type lostResponse struct {
	uploaderpb.TransferService_UploadServer
}

func (lostResponse) SendAndClose(*uploaderpb.UploadResponse) error { return nil }

type brokenUpload struct {
	uploaderpb.TransferService_UploadServer
	left int
}

func (b *brokenUpload) Recv() (*uploaderpb.UploadRequest, error) {
	if b.left == 0 {
		return nil, errInjected
	}
	b.left--
	return b.TransferService_UploadServer.Recv()
}

func (f *faultyServer) Download(req *uploaderpb.DownloadRequest, stream uploaderpb.TransferService_DownloadServer) error {
	f.downloadStreams.Add(1)
	if f.downloadFaults.Add(-1) < 0 {
		f.downloadFaults.Store(0)
		return f.Server.Download(req, stream)
	}
	// The metadata message plus faultAfterMessages chunks get through.
	_ = f.Server.Download(req, &brokenDownload{TransferService_DownloadServer: stream, left: 1 + faultAfterMessages})
	return status.Error(codes.Unavailable, "injected: connection lost")
}

type brokenDownload struct {
	uploaderpb.TransferService_DownloadServer
	left int
}

func (b *brokenDownload) Send(m *uploaderpb.DownloadResponse) error {
	if b.left == 0 {
		return errInjected
	}
	b.left--
	return b.TransferService_DownloadServer.Send(m)
}

func newTestClient(t *testing.T, fs *faultyServer) *Client {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewFilesystem(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := session.NewStore(filepath.Join(dir, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	fs.Server = server.New(store, sessions)

	gs := grpc.NewServer()
	uploaderpb.RegisterTransferServiceServer(gs, fs)
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
	t.Cleanup(func() {
		conn.Close()
		gs.Stop()
	})
	return NewFromConn(conn)
}

func noBackoff(t *testing.T) {
	t.Helper()
	old := retryBackoff
	retryBackoff = func(int) time.Duration { return 0 }
	t.Cleanup(func() { retryBackoff = old })
}

// writeSource writes n bytes of non-repeating content, so a chunk written at
// the wrong offset changes the result.
func writeSource(t *testing.T, n int) (string, []byte) {
	t.Helper()
	content := make([]byte, n)
	for i := range content {
		content[i] = byte(i*7 + i/251)
	}
	p := filepath.Join(t.TempDir(), "source.bin")
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatal(err)
	}
	return p, content
}

const chunk = 1000

// The bug in #4: a new upload that broke part-way returned no session ID.
func TestUploadFailureReturnsResumableSession(t *testing.T) {
	fs := &faultyServer{}
	fs.uploadFaults.Store(1)
	c := newTestClient(t, fs)
	src, content := writeSource(t, 10*chunk)
	ctx := context.Background()

	res, err := c.Upload(ctx, src, WithChunkSize(chunk), WithMaxRetries(0))
	if err == nil {
		t.Fatal("expected the injected failure, got success")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("error code = %v (%v), want Unavailable", status.Code(err), err)
	}
	if res == nil || res.SessionID == "" {
		t.Fatalf("failed upload returned no session ID: %+v", res)
	}

	// Resume by hand with that session.
	res2, err := c.Upload(ctx, src, WithChunkSize(chunk), WithSessionID(res.SessionID), WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	if !res2.Complete || res2.SessionID != res.SessionID {
		t.Fatalf("resume = %+v, want complete on session %s", res2, res.SessionID)
	}
	assertStored(t, c, res2.FileID, content)
}

func TestUploadRetriesTransientFailures(t *testing.T) {
	noBackoff(t)
	fs := &faultyServer{}
	fs.uploadFaults.Store(2)
	c := newTestClient(t, fs)
	src, content := writeSource(t, 10*chunk)

	res, err := c.Upload(context.Background(), src, WithChunkSize(chunk))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Complete {
		t.Fatalf("upload not complete: %+v", res)
	}
	if got := fs.uploadStreams.Load(); got != 3 {
		t.Errorf("upload streams = %d, want 3 (two failures, one success)", got)
	}
	assertStored(t, c, res.FileID, content)
}

func TestUploadStopsAfterMaxRetries(t *testing.T) {
	noBackoff(t)
	fs := &faultyServer{}
	fs.uploadFaults.Store(100)
	c := newTestClient(t, fs)
	src, _ := writeSource(t, 10*chunk)

	res, err := c.Upload(context.Background(), src, WithChunkSize(chunk), WithMaxRetries(2))
	if err == nil {
		t.Fatal("expected failure")
	}
	if got := fs.uploadStreams.Load(); got != 3 {
		t.Errorf("upload streams = %d, want 3 (one try plus two retries)", got)
	}
	if res == nil || res.SessionID == "" {
		t.Errorf("exhausted retries returned no session ID: %+v", res)
	}
}

// A server that predates StartUpload still accepts uploads.
func TestUploadWithoutStartUpload(t *testing.T) {
	fs := &faultyServer{noStartUpload: true}
	c := newTestClient(t, fs)
	src, content := writeSource(t, 5*chunk)

	res, err := c.Upload(context.Background(), src, WithChunkSize(chunk))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Complete || res.SessionID == "" {
		t.Fatalf("legacy upload = %+v, want complete with the server-assigned session", res)
	}
	assertStored(t, c, res.FileID, content)
}

func TestDownloadRetriesAndReturnsSession(t *testing.T) {
	noBackoff(t)
	fs := &faultyServer{}
	c := newTestClient(t, fs)
	// Larger than the server's 256 KB download chunk, so the stream breaks
	// part-way through the file.
	src, content := writeSource(t, 10*256*1024)
	up, err := c.Upload(context.Background(), src, WithChunkSize(64*1024))
	if err != nil || !up.Complete {
		t.Fatalf("upload: %+v %v", up, err)
	}

	fs.downloadFaults.Store(2)
	dest := filepath.Join(t.TempDir(), "out.bin")
	res, err := c.Download(context.Background(), up.FileID, dest)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Complete || res.SessionID == "" || res.Size != int64(len(content)) {
		t.Fatalf("download = %+v, want complete with a session and size %d", res, len(content))
	}
	if got := fs.downloadStreams.Load(); got != 3 {
		t.Errorf("download streams = %d, want 3 (two failures, one success)", got)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("downloaded %d bytes differ from the %d uploaded", len(got), len(content))
	}
}

func TestDownloadFailureReturnsResumableSession(t *testing.T) {
	fs := &faultyServer{}
	c := newTestClient(t, fs)
	src, content := writeSource(t, 10*256*1024)
	up, err := c.Upload(context.Background(), src, WithChunkSize(64*1024))
	if err != nil || !up.Complete {
		t.Fatalf("upload: %+v %v", up, err)
	}

	fs.downloadFaults.Store(1)
	dest := filepath.Join(t.TempDir(), "out.bin")
	res, err := c.Download(context.Background(), up.FileID, dest, WithMaxRetries(0))
	if err == nil {
		t.Fatal("expected the injected failure")
	}
	if res == nil || res.SessionID == "" || res.Complete {
		t.Fatalf("failed download = %+v, want an incomplete result with a session", res)
	}

	res2, err := c.Download(context.Background(), up.FileID, dest, WithSessionID(res.SessionID))
	if err != nil {
		t.Fatal(err)
	}
	if !res2.Complete {
		t.Fatalf("resumed download = %+v, want complete", res2)
	}
	got, _ := os.ReadFile(dest)
	if !bytes.Equal(got, content) {
		t.Fatalf("resumed download differs from the uploaded content")
	}
}

func assertStored(t *testing.T, c *Client, fileID string, want []byte) {
	t.Helper()
	dest := filepath.Join(t.TempDir(), "check.bin")
	if _, err := c.Download(context.Background(), fileID, dest); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("stored file (%d bytes) differs from the source (%d bytes)", len(got), len(want))
	}
}

// A large upload is still sending when the server fails, so Send reports only
// io.EOF; the retry decision needs the real status behind it.
func TestUploadRetriesWhenSendFails(t *testing.T) {
	noBackoff(t)
	fs := &faultyServer{}
	fs.uploadFaults.Store(1)
	c := newTestClient(t, fs)
	src, content := writeSource(t, 8<<20)

	res, err := c.Upload(context.Background(), src, WithChunkSize(chunk))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Complete {
		t.Fatalf("upload not complete: %+v", res)
	}
	if got := fs.uploadStreams.Load(); got != 2 {
		t.Errorf("upload streams = %d, want 2 (one failure, one success)", got)
	}
	assertStored(t, c, res.FileID, content)
}

// The upload finished on the server but the response was lost: the retry
// resumes from the committed end of a session that is already complete, and
// must report success rather than fail to finalize the file a second time.
func TestUploadRetryAfterLostResponse(t *testing.T) {
	noBackoff(t)
	fs := &faultyServer{}
	fs.lostResponses.Store(1)
	c := newTestClient(t, fs)
	src, content := writeSource(t, 10*chunk)

	res, err := c.Upload(context.Background(), src, WithChunkSize(chunk))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Complete || res.FileID == "" {
		t.Fatalf("upload = %+v, want complete with a file ID", res)
	}
	if got := fs.uploadStreams.Load(); got != 2 {
		t.Errorf("upload streams = %d, want 2", got)
	}
	assertStored(t, c, res.FileID, content)
}
