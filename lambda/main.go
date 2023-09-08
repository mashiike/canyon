package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/mashiike/canyon"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer cancel()

	opts := []canyon.Option{
		canyon.WithServerAddress(":8080", "/"),
		canyon.WithVarbose(),
	}
	if os.Getenv("CANYON_S3_BACKEND") != "" {
		b, err := canyon.NewS3Backend(os.Getenv("CANYON_S3_BACKEND"))
		if err != nil {
			slog.Error("failed to create s3 backend", "error", err)
			os.Exit(1)
		}
		b.SetUploaderName("canyon-example")
		opts = append(opts, canyon.WithBackend(b))
	}
	opts = append(opts, canyon.WithCanyonEnv(os.Getenv("CANYON_ENV")))
	err := canyon.RunWithContext(ctx, "canyon-example", http.HandlerFunc(handler), opts...)
	if err != nil {
		slog.Error("failed to run canyon", "error", err)
		os.Exit(1)
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	logger := canyon.Logger(r)
	if !canyon.IsWorker(r) {
		logger.Info("server process", slog.String("request", r.URL.Path))
		// handle webhook directly
		messageId, err := canyon.SendToWorker(r, nil)
		if err != nil {
			logger.Error("failed to send sqs message", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		logger.Info("send sqs message", slog.String("message_id", messageId), slog.String("method", r.Method), slog.String("path", r.URL.Path))
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// handle from sqs message
	logger.Info("worker process", slog.String("request", r.URL.Path))
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("failed to read body", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	logger.Info("event request body", slog.String("body", string(bs)))
	w.WriteHeader(http.StatusOK) // if 2xx is success, sqs message will be deleted
}
