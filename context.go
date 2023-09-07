package canyon

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type contextKey string

var (
	contextKeyLogger           = contextKey("logger")
	contextKeySQSMessageSender = contextKey("sqs-message-sender")
	contextKeyIsWorker         = contextKey("is-worker")
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

type sqsMessageSender func(*http.Request, map[string]types.MessageAttributeValue) (string, error)

func sqsMessageSenderFromContext(ctx context.Context) sqsMessageSender {
	sender, ok := ctx.Value(contextKeySQSMessageSender).(sqsMessageSender)
	if !ok {
		return nil
	}
	return sender
}

func embedSQSMessageSenderInContext(ctx context.Context, sender sqsMessageSender) context.Context {
	return context.WithValue(ctx, contextKeySQSMessageSender, sender)
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
