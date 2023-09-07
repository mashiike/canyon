# canyon
Go net/http integration for job queue worker pattern with AWS Lambda and AWS SQS 

[![GoDoc](https://godoc.org/github.com/mashiike/canyon?status.svg)](https://godoc.org/github.com/mashiike/canyon)
[![Go Report Card](https://goreportcard.com/badge/github.com/mashiike/canyon)](https://goreportcard.com/report/github.com/mashiike/canyon)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## Example

```go
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
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("failed to read body", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	logger.Info("event request body", slog.String("body", string(bs)))
	w.WriteHeader(http.StatusOK) // if 2xx is success, sqs message will be deleted
}
```

example lambda function in [lambda/](lambda/) directory.

### canyon.RunWithContext(ctx, sqs_queue_name, handler, opts...)

`canyon.RunWithContext(ctx, sqs_queue_name, handler, opts...)` works as below.

- If a process is running on Lambda (`AWS_EXECUTION_ENV` or `AWS_LAMBDA_RUNTIME_API` environment variable defined),
  - Call lambda.Start()
  - if AWS Lambda invoke request has `Records` field, call handler as worker.
  - if AWS Lambda invoke request as HTTP integration, call handler as server.
- Otherwise start two go routines
  - HTTP server is a net/http server
  - SQS worker is a sqs long polling worker for sqs_queue_name,

### canyon.IsWorker(r)

`canyon.IsWorker(r)` returns true if the request is from SQS worker.

if this functions returns false, handler behaves as webhook handling server.
if not worker request, `canyon.SendToWorker(r, nil)` sends request to SQS queue.

if this functions returns true, handler behaves as worker.
canyon convert SQS Event to HTTP Request, and set `Sqs-Message-Id`, `Sqs-Message-Attributes-...` header to request.

### canyon.SendToWorker(r, attributes)

`canyon.SendToWorker(r, attributes)` sends request to SQS queue.
can call only `canyon.IsWorker(r) == false` request.
this function is capsuled `sqsClient.SendMessage(ctx, &sqs.SendMessageInput{...})` and returns `SendMessageOutput.MessageId` and `error`.

if attributes is nil, sqs message no message attributes.
can set `map[string]sqs.MessageAttributeValue` to attributes.
helper function `canyon.ToMessageAttributes(...)` converts http.Header to sqs.MessageAttributeValue.

## Advanced Usage

### If customizing worker response behavior, use `canyon.WithWorkerResponseChecker`

```go
package main

//...

func main() {
//...
	opts := []canyon.Option{
		canyon.WithServerAddress(":8080", "/"),
		canyon.WithWrokerResponseChecker(canyon.WorkerResponseCheckerFunc(
			func(_ context.Context, r *http.Response) bool {
				// this function called end of worker process
				return r.StatusCode != http.StatusOK //return isFailed flag
			},
		)),
	}
	err := canyon.RunWithContext(ctx, "canyon-example", http.HandlerFunc(handler), opts...)
	if err != nil {
		slog.Error("failed to run canyon", "error", err)
		os.Exit(1)
	}
}
```

if return true, sqs message will not be deleted.

## LICENSE

MIT
