package sdk

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	uploaderpb "github.com/trendvidia/uploader/proto/uploader/v1"
)

// Client provides upload and download operations against the transfer service.
type Client struct {
	conn *grpc.ClientConn
	rpc  uploaderpb.TransferServiceClient
}

// New creates a client connected to the given address.
func New(addr string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		rpc:  uploaderpb.NewTransferServiceClient(conn),
	}, nil
}

// NewFromConn creates a client from an existing gRPC connection.
func NewFromConn(conn *grpc.ClientConn) *Client {
	return &Client{
		conn: conn,
		rpc:  uploaderpb.NewTransferServiceClient(conn),
	}
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
