package server

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClientGone(t *testing.T) {
	gone := []error{
		context.Canceled,
		status.Error(codes.Canceled, "x"),
		status.Error(codes.DeadlineExceeded, "x"),
		status.Error(codes.Unavailable, "x"),
	}
	for _, err := range gone {
		if !clientGone(err) {
			t.Errorf("clientGone(%v) = false, want true", err)
		}
	}
	notGone := []error{
		errors.New("disk full"),
		status.Error(codes.Internal, "x"),
		status.Error(codes.InvalidArgument, "x"),
		nil,
	}
	for _, err := range notGone {
		if clientGone(err) {
			t.Errorf("clientGone(%v) = true, want false", err)
		}
	}
}

// streamError returns the transport error unchanged, so the client sees its
// real gRPC code rather than a generic Internal.
func TestStreamErrorPreservesCode(t *testing.T) {
	in := status.Error(codes.Canceled, "client went away")
	if out := streamError("recv chunk", in); out != in {
		t.Errorf("streamError changed the error: %v", out)
	}
}
