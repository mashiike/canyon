package canyon_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mashiike/canyon"
)

func Example() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		slog.Error("listen failed", "error", err)
		os.Exit(1)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		slog.Error("split host port failed", "error", err)
		os.Exit(1)
	}
	defer listener.Close()
	slog.Info("listen start", "port", port)

	var wg sync.WaitGroup
	var messageCh = make(chan string, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := canyon.Logger(r)
			if !canyon.IsWorker(r) {
				logger.Info("handle webhook directly", "method", r.Method, "path", r.URL.Path)
				// handle webhook directly
				messageId, err := canyon.SendToWorker(r, canyon.ToMessageAttributes(
					http.Header{
						"User-Name": []string{"test user"},
					},
				))
				if err != nil {
					logger.Error("failed to send sqs message", "error", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				logger.Info("send sqs message", "message_id", messageId, "method", r.Method, "path", r.URL.Path)
				w.WriteHeader(http.StatusAccepted)
				return
			}
			// handle from sqs message
			logger.Info("handle webhook directly", "method", r.Method, "path", r.URL.Path, "message_id", r.Header.Get(canyon.HeaderSQSMessageId))
			bs, err := io.ReadAll(r.Body)
			if err != nil {
				logger.Error("failed to read body", "error", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer r.Body.Close()
			messageCh <- string(bs) + ":" + r.Header.Get(canyon.HeaderSQSMessageAttribute("UserName", "String"))
			close(messageCh)
			w.WriteHeader(http.StatusOK) // if return 2xx, sqs message will be deleted from queue
		})
		opts := []canyon.Option{
			canyon.WithListener(listener, "/"),
			canyon.WithInMemoryQueue(
				30*time.Second, // on memory queue's default visibility timeout
				10,             // on memory queue's default max receive count,
				os.Stdout,      // if exceed max receive count, message will be sent to stdout as json
			),
			canyon.WithVarbose(),
		}
		err := canyon.RunWithContext(ctx, "test-queue", h, opts...)
		if err != nil && err != context.Canceled {
			slog.Error("run failed", "error", err)
			return
		}
	}()
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%s/", port), "text/plain", strings.NewReader("hello world"))
	if err != nil {
		slog.Error("post failed", "error", err)
		os.Exit(1)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		slog.Error("post failed", "status", resp.StatusCode)
		os.Exit(1)
	} else {
		slog.Info("post success", "status", resp.StatusCode)
	}
	for msg := range messageCh {
		slog.Info("received message", "message", msg)
		fmt.Println(msg)
	}
	cancel()
	wg.Wait()
	//Output:
	// hello world:test user
}
