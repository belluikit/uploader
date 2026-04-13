package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	uploaderpb "github.com/trendvidia/uploader/proto/uploader/v1"
	"github.com/trendvidia/uploader/server"
	"github.com/trendvidia/uploader/session"
	"github.com/trendvidia/uploader/storage"
)

func main() {
	root := &cobra.Command{
		Use:   "uploader",
		Short: "Resumable file transfer service",
	}

	var (
		listen  string
		dataDir string
	)

	serve := &cobra.Command{
		Use:   "serve",
		Short: "Start the gRPC transfer server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServe(cmd.Context(), listen, dataDir)
		},
	}
	serve.Flags().StringVar(&listen, "listen", ":9090", "gRPC listen address")
	serve.Flags().StringVar(&dataDir, "data-dir", "data", "base directory for files and sessions")
	root.AddCommand(serve)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := root.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}

func runServe(ctx context.Context, listen, dataDir string) error {
	store, err := storage.NewFilesystem(dataDir)
	if err != nil {
		return fmt.Errorf("create storage: %w", err)
	}

	sessions, err := session.NewStore(dataDir)
	if err != nil {
		return fmt.Errorf("create session store: %w", err)
	}

	srv := server.New(store, sessions)
	srv.StartSweeper(ctx, 1*time.Hour)

	grpcServer := grpc.NewServer()
	uploaderpb.RegisterTransferServiceServer(grpcServer, srv)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	slog.Info("server starting", "addr", lis.Addr().String())

	errCh := make(chan error, 1)
	go func() { errCh <- grpcServer.Serve(lis) }()

	select {
	case <-ctx.Done():
		slog.Info("shutting down")
		grpcServer.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}
