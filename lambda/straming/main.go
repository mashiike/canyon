package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mashiike/canyon"
)

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
		canyon.WithStreamingResponse(),
	}
	err := canyon.RunWithContext(ctx, "canyon-example", http.HandlerFunc(handler), opts...)
	if err != nil {
		slog.Error("failed to run canyon", "error", err)
		os.Exit(1)
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	// write initial data
	w.Write([]byte("data: Initial data\n\n"))
	flusher.Flush()
	// write data every 1 second
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		fmt.Fprintf(w, "data: %d\n\n", i)
		flusher.Flush()
	}
	// write final data
	w.Write([]byte("data: Final data\n\n"))
	flusher.Flush()
}
