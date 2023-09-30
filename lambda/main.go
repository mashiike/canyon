package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mashiike/canyon"
)

var randReader = rand.New(rand.NewSource(time.Now().UnixNano()))

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer cancel()
	os.Setenv("CANYON_SCHEDULER", "true")
	opts := []canyon.Option{
		canyon.WithServerAddress(":8080", "/"),
		canyon.WithCanyonEnv("CANYON_"), // environment variables prefix
		canyon.WithLambdaFallbackHandler(func(ctx context.Context, event json.RawMessage) error {
			slog.Error("non SQS or HTTP event", "event", string(event))
			return errors.New("invalid lambda payload")
		}),
	}
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
	headerAttrs := make([]slog.Attr, 0, len(r.Header))
	for k := range r.Header {
		headerAttrs = append(headerAttrs, slog.String(k, r.Header.Get(k)))
	}
	logger.Debug("request header", "header", slog.GroupValue(headerAttrs...))
	if randReader.Float64() < 0.5 {
		logger.Info("randomly failed")
		retryAfter := 1 + randReader.Int31n(10)
		w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("failed to read body", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	logger.Info("event request body", slog.String("body", string(bs)))
	w.WriteHeader(http.StatusOK) // if 2xx is success, sqs message will be deleted
}
