// Package sdk provides a client library for the file transfer service.
package sdk

// ProgressFunc is called during upload/download with the current byte offset
// and total file size.
type ProgressFunc func(current, total int64)

// UploadResult contains the outcome of a completed or partial upload.
type UploadResult struct {
	SessionID     string
	FileID        string
	CommittedSize int64
	Complete      bool
}

// Option configures upload/download behavior.
type Option func(*options)

type options struct {
	chunkSize  int
	progress   ProgressFunc
	sessionID  string
	maxRetries int
}

func defaultOptions() options {
	return options{
		chunkSize:  256 * 1024, // 256 KB
		maxRetries: 3,
	}
}

// WithChunkSize sets the chunk size in bytes for uploads.
func WithChunkSize(n int) Option {
	return func(o *options) { o.chunkSize = n }
}

// WithProgress sets a callback invoked after each chunk transfer.
func WithProgress(fn ProgressFunc) Option {
	return func(o *options) { o.progress = fn }
}

// WithSessionID sets a session ID for resuming a previous transfer.
func WithSessionID(id string) Option {
	return func(o *options) { o.sessionID = id }
}

// WithMaxRetries sets the maximum number of automatic retry attempts
// on transient errors. Default is 3. Set to 0 to disable retries.
func WithMaxRetries(n int) Option {
	return func(o *options) { o.maxRetries = n }
}
