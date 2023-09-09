package canyontest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/mashiike/canyon"
	"github.com/mashiike/canyon/canyontest"
	"github.com/stretchr/testify/require"
)

func TestRunner(t *testing.T) {
	workerRequestCh := make(chan *http.Request, 1)
	fallbackJSONCh := make(chan json.RawMessage, 1)
	r := canyontest.NewRunner(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !canyon.IsWorker(r) {
				slog.Info("server process", slog.String("request", r.URL.Path))
				// handle webhook directly
				canyon.SendToWorker(r, nil)
				w.WriteHeader(http.StatusAccepted)
				return
			}
			// handle from sqs message
			slog.Info("worker process", slog.String("request", r.URL.Path))
			workerRequestCh <- r
			w.WriteHeader(http.StatusOK) // if 2xx is success, sqs message will be deleted
		}),
		canyon.WithVarbose(),
		canyon.WithLambdaFallbackHandler(func(ctx context.Context, event json.RawMessage) error {
			slog.Info("fallback", slog.String("event", string(event)))
			fallbackJSONCh <- event
			return nil
		}),
	)
	defer r.Close()

	resp, err := http.Post(r.URL, "application/json", strings.NewReader(`{"foo":"bar baz"}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	workerRequest := <-workerRequestCh
	if workerRequest.Method != "POST" {
		t.Errorf("unexpected method: %s", workerRequest.Method)
	}
	if workerRequest.URL.Path != "/" {
		t.Errorf("unexpected path: %s", workerRequest.URL.Path)
	}

	fmt.Fprint(r.Stdin, `hogehoge {"hoge":"fuga"}\n`)
	fallbackJSON := <-fallbackJSONCh
	require.JSONEq(t, `{"hoge":"fuga"}`, string(fallbackJSON))
}
