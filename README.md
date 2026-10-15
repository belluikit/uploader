# uploader

A resumable file transfer service over gRPC, with a Go client SDK. A client
streams a file to the server in chunks; if the transfer is interrupted, it can
ask the server how much was committed and continue from there instead of
starting over. Downloads work the same way in the other direction. Both sides
use a SHA-256 of the file to notice when it changed between attempts.

The repository has:

| Path | What it is |
|---|---|
| `proto/uploader/v1` | The `TransferService` protobuf definition and its generated Go code (package `uploaderpb`) |
| `server` | The `TransferService` implementation |
| `session` | Upload/download session state, persisted to disk |
| `storage` | The storage backend interface and a local-filesystem implementation |
| `sdk` | A Go client for the service |
| `cmd/uploader` | The server binary |

## Install

Requires Go 1.26.1 or later (see `go.mod`).

The SDK, for a Go client:

```sh
go get github.com/belluikit/uploader
```

The server binary:

```sh
go install github.com/belluikit/uploader/cmd/uploader@latest
```

## Running the server

```sh
uploader serve --listen :9090 --data-dir data
```

| Flag | Default | Meaning |
|---|---|---|
| `--listen` | `:9090` | gRPC listen address |
| `--data-dir` | `data` | Base directory for files and sessions |

The server listens on plaintext gRPC (no TLS is configured) and registers gRPC
server reflection. It stops gracefully on interrupt (Ctrl-C). Every hour it
sweeps expired sessions; a session expires 24 hours after its last update
(`session.DefaultTTL`).

Under `--data-dir` it creates four directories:

- `staging/` — uploads in progress, one file per file ID
- `files/` — completed uploads, named by file ID
- `upload/`, `download/` — session state, one `<session-id>.session` file each

The original filename is recorded in the upload session, not in the stored
file's name.

## Using the Go SDK

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/belluikit/uploader/sdk"
)

func main() {
	c, err := sdk.New("localhost:9090") // plaintext unless you pass grpc.DialOptions
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	res, err := c.Upload(ctx, "video.mp4",
		sdk.WithChunkSize(1<<20),
		sdk.WithProgress(func(cur, total int64) { fmt.Printf("\r%d/%d", cur, total) }),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nfile id:", res.FileID, "session:", res.SessionID, "complete:", res.Complete)

	if err := c.Download(ctx, res.FileID, "copy.mp4"); err != nil {
		log.Fatal(err)
	}
}
```

- `sdk.New(addr, opts...)` dials the server. With no `grpc.DialOption`s it
  uses insecure (plaintext) credentials; pass your own to change that.
  `sdk.NewFromConn` wraps an existing `*grpc.ClientConn`.
- `Client.Upload(ctx, path, opts...)` returns an `*UploadResult` with
  `SessionID`, `FileID`, `CommittedSize` and `Complete`.
- `Client.Download(ctx, fileID, destPath, opts...)` writes the file to
  `destPath`.

Options:

| Option | Effect |
|---|---|
| `WithChunkSize(n)` | Upload chunk size in bytes (default 256 KB) |
| `WithProgress(fn)` | Called after each chunk with bytes so far and the total |
| `WithSessionID(id)` | Resume a previous upload or download session |
| `WithMaxRetries(n)` | Accepted and stored, but not yet read by `Upload` or `Download` — it does not currently retry anything |

## How resuming works

**Upload.** Pass the earlier `UploadResult.SessionID` with `WithSessionID`.
The SDK calls `QueryUpload` first:

- If the session is unknown or expired, it starts a new upload.
- If the local file's SHA-256 no longer matches the one the session recorded,
  it starts a new upload.
- Otherwise it seeks to the session's committed offset and sends only the rest.

The server saves the committed offset after every chunk it writes, and moves
the file from `staging/` to `files/` once the committed offset reaches the
declared file size.

The server returns the session ID only when the upload stream closes normally.
So an upload that fails on its first attempt comes back with an empty
`SessionID` in its `UploadResult`, and that attempt cannot be resumed through
the SDK.

**Download.** Pass a download session ID with `WithSessionID` and the same
`destPath`. If `QueryDownload` reports the session as live, the SDK resumes from
the current size of the file at `destPath` and sends the session's hash. The
server refuses with `FailedPrecondition` if the stored file's hash no longer
matches. The SDK's `Download` does not return the session ID, so it is
available only to callers that use the gRPC API directly (the first
`DownloadMetadata` message carries it).

## gRPC API

`TransferService`, defined in
[`proto/uploader/v1/uploader.proto`](proto/uploader/v1/uploader.proto):

| RPC | Shape | Purpose |
|---|---|---|
| `Upload` | client stream → `UploadResponse` | First message is `UploadMetadata` (session ID or empty, filename, size, SHA-256, resume offset); the rest are `UploadChunk`s |
| `Download` | `DownloadRequest` → server stream | First message is `DownloadMetadata` (session ID, size, SHA-256); the rest are `DownloadChunk`s |
| `QueryUpload` | unary | Committed offset, file hash and expiry of an upload session |
| `QueryDownload` | unary | File ID, downloaded offset, file hash and expiry of a download session |

Any gRPC client can use the service; the Go SDK is one such client.

## Storage backends

`storage.Backend` is the interface the server stores files through.
The repository ships one implementation, `storage.Filesystem`
(`storage.NewFilesystem(baseDir)`), which keeps files on local disk. The
`uploader` binary always uses it. To store files elsewhere, implement
`storage.Backend` and pass it to `server.New` in your own binary.

## Platforms

Pure Go. There are no build-tagged files, so it builds wherever Go and gRPC do.

## License

BSD-3-Clause. See [LICENSE](LICENSE).

Part of the [Bellui](https://github.com/belluikit/bellui) Go UI toolkit
ecosystem — <https://bellui.com>.
