package sdk

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryBackoff is the wait before retry attempt n (0-based): 200ms doubling
// up to 5s. A variable so tests can make it instant.
var retryBackoff = func(attempt int) time.Duration {
	d := 200 * time.Millisecond << attempt
	if d <= 0 || d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

// isRetryable reports whether err is a transient failure worth resuming
// after. The caller's own cancellation or deadline is never retried.
func isRetryable(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.DeadlineExceeded:
		return true
	}
	return false
}

// sleepCtx waits for d, or returns early with ctx's error.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
