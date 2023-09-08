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
    err := canyon.RunWithContext(ctx, "your-sqs-queue-name", http.HandlerFunc(handler), opts...)
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

`canyon.SendToWorker(r, attributes)` sends request to worker with SQS queue.
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
    err := canyon.RunWithContext(ctx, "your-sqs-queue-name", http.HandlerFunc(handler), opts...)
    if err != nil {
        slog.Error("failed to run canyon", "error", err)
        os.Exit(1)
    }
}
```

if return true, sqs message will not be deleted.

### Large Payload, Request Body upload to S3

```go
package main

//...

func main() {
//...
    b, err := canyon.NewS3Backend("s3://bucket-name/prefix")
    if err != nil {
        slog.Error("failed to create s3 backend", "error", err)
        os.Exit(1)
    }
    b.SetUploaderName("your-app-name") // if not set, default is "canyon"
    opts := []canyon.Option{
        canyon.WithServerAddress(":8080", "/"),
        canyon.WithBackend(b),
    }
    err := canyon.RunWithContext(ctx, "your-sqs-queue-name", http.HandlerFunc(handler), opts...)
    if err != nil {
        slog.Error("failed to run canyon", "error", err)
        os.Exit(1)
    }
}
```

if request body size is over 256KB, SQS Send Message API returns error.
this case, use `canyon.WithBackend` option.
if this option is set, `canyon.IsWorker(r) == false` request, request body will be upload to Backend.
and `canyon.IsWorker(r) == true` request, request body will be download from Backend.

`canyon.NewS3Backend("s3://bucket-name/prefix")` returns `canyon.S3Backend` instance.
this instance is implementation of `canyon.Backend` interface with AWS S3. 

## For testing  

`caynontest` package is helper package for testing.
this package like `httptest` package.
for example

```go
func TestXXX(t *testing.T) {
    h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // your handler code
        // can use canyon.IsWorker(r) and canyon.SendToWorker(r, nil)
	})
	r := canyontest.NewRunner(h)
	defer r.Close()

	resp, err := http.Post(r.URL, "application/json", strings.NewReader(`{"foo":"bar baz"}`))
	if err != nil {
		t.Fatal(err)
	}
    // your test code
}
```

if you want to only handler test, use `canyontest.AsServer(h)` and `canontest.AsWorker(h)`.
this is middleware for handler testing. not start real http server and sqs worker.

```go
func TestServerLogic(t *testing.T) {
    h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // your server logic code
        // canyon.SendToWorker(r, nil)
        // canyon.IsWorker(r) == false
    })
    sender := canyon.SQSMessageSenderFunc(func(r *http.Request, m canyon.MessageAttributes) (string, error) {
        // call from canyon.SendToWorker()
        return "message-id", nil
    })
    h = canyontest.AsServer(h)
    r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"foo":"bar baz"}`))
    w := httptest.NewRecorder()
    h.ServeHTTP(w, r)
    // your test code
}

func TestWorkerLogic(t *testing.T) {
    h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // your worker logic code
        // canyon.IsWorker(r) == true
        // r.Header with Sqs-Message-Id, Sqs-Message-Attributes-... headers
    })
    h = canyontest.AsWorker(h)
    r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"foo":"bar baz"}`))
    w := httptest.NewRecorder()
    h.ServeHTTP(w, r)
    // your test code
}
```

## LICENSE

MIT
