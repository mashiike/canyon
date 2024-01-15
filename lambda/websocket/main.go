package main

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/mashiike/canyon"
)

var index []byte
var connectionIDs = make(map[string]struct{})

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	tmpl, err := template.ParseFiles("index.html")
	if err != nil {
		slog.Error("failed to parse template", "error", err)
		os.Exit(1)
	}
	websocketURL := os.Getenv("WEBSOCKET_URL")
	if websocketURL == "" {
		websocketURL = "ws://localhost:8081"
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, map[string]string{
		"WEBSOCKET_URL": websocketURL,
	})
	if err != nil {
		slog.Error("failed to execute template", "error", err)
		os.Exit(1)
	}
	index = buf.Bytes()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer cancel()
	opts := []canyon.Option{
		canyon.WithServerAddress(":8080", "/"),
		canyon.WithWebsocketAddress(":8081"),
		canyon.WithCanyonEnv("CANYON_"), // environment variables prefix
	}
	err = canyon.RunWithContext(ctx, "canyon-websocket-example", http.HandlerFunc(handler), opts...)
	if err != nil {
		slog.Error("failed to run canyon", "error", err)
		os.Exit(1)
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	logger := canyon.Logger(r)
	logger.Debug("request",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("connection_id", canyon.WebsocketConnectionID(r)),
		slog.String("route_key", canyon.WebsocketRouteKey(r)),
		slog.Bool("is_websocket", canyon.IsWebsocket(r)),
		slog.Bool("is_worker", canyon.IsWorker(r)),
	)
	if !canyon.IsWebsocket(r) {
		logger.Info("server process", slog.String("request", r.URL.Path))
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		w.Write(index)
		return
	}
	connectionID := canyon.WebsocketConnectionID(r)
	switch canyon.WebsocketRouteKey(r) {
	case "$connect":
		logger.Info("websocket connect", slog.String("connection_id", connectionID))
		connectionIDs[connectionID] = struct{}{}
		w.WriteHeader(http.StatusOK)
		return
	case "$disconnect":
		logger.Info("websocket disconnect", slog.String("connection_id", connectionID))
		delete(connectionIDs, connectionID)
		w.WriteHeader(http.StatusOK)
		return
	case "$default":
		logger.Info("websocket default", slog.String("connection_id", connectionID))
		connectionIDs[connectionID] = struct{}{}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error("failed to read body", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to read body: %s", err)
			return
		}
		if canyon.IsWorker(r) {
			// handle from sqs message
			logger.Info("worker process", "connection_id", connectionID, "connections", len(connectionIDs))
			var buf bytes.Buffer
			fmt.Fprintf(&buf, "[%s]: %s", connectionID, body)
			if err := broadcast(r.Context(), connectionID, buf.Bytes()); err != nil {
				logger.Error("failed to broadcast", "error", err)
				w.WriteHeader(http.StatusInternalServerError)
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		msgID, err := canyon.SendToWorker(r, nil)
		if err != nil {
			logger.Error("failed to send sqs message", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to send sqs message: %s", err)
			return
		}
		logger.Info("send sqs message", slog.String("message_id", msgID), slog.String("method", r.Method), slog.String("path", r.URL.Path))
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "[%s]: %s", connectionID, body)
		return
	}
	w.WriteHeader(http.StatusNotFound)
	fmt.Fprint(w, http.StatusText(http.StatusNotFound))
}

func broadcast(ctx context.Context, sender string, body []byte) error {
	deleteList := make([]string, 0, len(connectionIDs))
	for target := range connectionIDs {
		if target == sender {
			continue
		}
		err := canyon.PostToConnection(ctx, target, body)
		if err != nil {
			if canyon.ConnectionIsGone(err) {
				deleteList = append(deleteList, target)
				continue
			}
			return err
		}
	}
	for _, target := range deleteList {
		delete(connectionIDs, target)
	}
	return nil
}
