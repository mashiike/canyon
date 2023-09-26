package canyon_test

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/mashiike/canyon"
	"github.com/mashiike/canyon/canyontest"
)

func Example() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	var messageCh = make(chan string, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger := canyon.Logger(r)
		if !canyon.IsWorker(r) {
			logger.Info("handle webhook directly", "method", r.Method, "path", r.URL.Path)
			// handle webhook directly
			messageId, err := canyon.SendToWorker(r, &canyon.SendOptions{
				MessageAttributes: canyon.ToMessageAttributes(
					http.Header{
						"User-Name": []string{"test user"},
					},
				),
			})
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
		logger.Info("handle webhook directly", "method", r.Method, "path", r.URL.Path, "message_id", r.Header.Get(canyon.HeaderSQSMessageID))
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
	r := canyontest.NewRunner(h, canyon.WithVarbose())
	defer r.Close()
	resp, err := http.Post(r.URL, "text/plain", strings.NewReader("hello world"))
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
	//Output:
	// hello world:test user
}
