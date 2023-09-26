package canyon

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
)

type contextKey string

var (
	contextKeyLogger            = contextKey("logger")
	contextKeyWorkerSender      = contextKey("sqs-message-sender")
	contextKeyIsWorker          = contextKey("is-worker")
	contextKeyBackupRequestBody = contextKey("backup-request-body")
)

// Logger returns slog.Logger with component attribute.
// if called by sqs message, component is "worker".
// if called original http request, component is "server".
func Logger(r *http.Request) *slog.Logger {
	ctx := r.Context()
	if ctx == nil {
		return slog.Default()
	}
	logger, ok := ctx.Value(contextKeyLogger).(*slog.Logger)
	if !ok {
		return slog.Default()
	}
	return logger
}

func embedLoggerInContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, contextKeyLogger, logger)
}

func workerSenderFromContext(ctx context.Context) WorkerSender {
	sender, ok := ctx.Value(contextKeyWorkerSender).(WorkerSender)
	if !ok {
		return nil
	}
	return sender
}

// EmbedWorkerSenderInContext embeds WorkerSender in context.
// for testing, not for production use.
func EmbedWorkerSenderInContext(ctx context.Context, sender WorkerSender) context.Context {
	return context.WithValue(ctx, contextKeyWorkerSender, sender)
}

// IsWorker returns true if the request is from worker.
// if running with canyon and http.Handler called from sqs message, return true.
func IsWorker(r *http.Request) bool {
	ctx := r.Context()
	if ctx == nil {
		return false
	}
	isWorker, ok := ctx.Value(contextKeyIsWorker).(bool)
	if !ok {
		return false
	}
	return isWorker
}

// EmbedIsWorkerInContext embeds isWorker flag in context.
//
//	this function is for http.Handler unit testing.
//	not for production use.
func EmbedIsWorkerInContext(ctx context.Context, isWorker bool) context.Context {
	return context.WithValue(ctx, contextKeyIsWorker, isWorker)
}

// BackupRequestBody returns backuped request body.
// maybe http handler read request body, and serializer read request body.
func BackupRequset(r *http.Request) (*http.Request, func() error, error) {
	cloned := r.Clone(r.Context())
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, r.Body.Close, err
	}
	cloned.Body = io.NopCloser(bytes.NewReader(body))
	ctx := context.WithValue(cloned.Context(), contextKeyBackupRequestBody, body)
	cloned = cloned.WithContext(ctx)
	return cloned, r.Body.Close, err
}

// RestoreRequest restores request body from backuped request body.
func RestoreRequest(r *http.Request) *http.Request {
	ctx := r.Context()
	if ctx == nil {
		return r
	}
	body, ok := ctx.Value(contextKeyBackupRequestBody).([]byte)
	if !ok {
		return r
	}
	cloned := r.Clone(ctx)
	cloned.Body = io.NopCloser(bytes.NewReader(body))
	return cloned
}
