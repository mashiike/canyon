package canyon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi"
	"github.com/aws/smithy-go"
)

type contextKey string

var (
	contextKeyLogger                      = contextKey("logger")
	contextKeyWorkerSender                = contextKey("sqs-message-sender")
	contextKeyIsWorker                    = contextKey("is-worker")
	contextKeyBackupRequestBody           = contextKey("backup-request-body")
	contextKeyWebsocketManagmentAPIClient = contextKey("websocket-management-api-client")
)

// Logger returns slog.Logger with component attribute.
// if called by sqs message, component is "worker".
// if called original http request, component is "server".
func Logger(r *http.Request) *slog.Logger {
	logger, _ := loggerFromContext(r.Context())
	return logger
}

func loggerFromContext(ctx context.Context) (*slog.Logger, bool) {
	if ctx == nil {
		return slog.Default(), false
	}
	logger, ok := ctx.Value(contextKeyLogger).(*slog.Logger)
	if !ok {
		return slog.Default(), false
	}
	return logger, true
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

func websocketManagementAPIClientFromContext(ctx context.Context) (ManagementAPIClient, bool) {
	client, ok := ctx.Value(contextKeyWebsocketManagmentAPIClient).(ManagementAPIClient)
	return client, ok
}

// EmbedWebsocketManagementAPIClient embeds WebsocketManagementAPIClient in context.
// for testing, not for production use.
func EmbedWebsocketManagementAPIClient(ctx context.Context, client ManagementAPIClient) context.Context {
	return context.WithValue(ctx, contextKeyWebsocketManagmentAPIClient, client)
}

// PostToConnection posts data to connectionID.
func PostToConnection(ctx context.Context, connectionID string, data []byte) error {
	client, ok := websocketManagementAPIClientFromContext(ctx)
	if !ok {
		return errors.New("websocket management api client not found in context")
	}
	_, err := client.PostToConnection(ctx, &apigatewaymanagementapi.PostToConnectionInput{
		ConnectionId: aws.String(connectionID),
		Data:         data,
	})
	return err
}

// DeleteConnection deletes connectionID.
func DeleteConnection(ctx context.Context, connectionID string) error {
	client, ok := websocketManagementAPIClientFromContext(ctx)
	if !ok {
		return errors.New("websocket management api client not found in context")
	}
	_, err := client.DeleteConnection(ctx, &apigatewaymanagementapi.DeleteConnectionInput{
		ConnectionId: aws.String(connectionID),
	})
	return err
}

// GetConnection gets connectionID.
func GetConnection(ctx context.Context, connectionID string) (*apigatewaymanagementapi.GetConnectionOutput, error) {
	client, ok := websocketManagementAPIClientFromContext(ctx)
	if !ok {
		return nil, errors.New("websocket management api client not found in context")
	}
	return client.GetConnection(ctx, &apigatewaymanagementapi.GetConnectionInput{
		ConnectionId: aws.String(connectionID),
	})
}

// ConnectionIsGone returns true if err is GoneException.
func ConnectionIsGone(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "GoneException"
}

// ExitsConnection returns true if connectionID exists.
func ExitsConnection(ctx context.Context, connectionID string) (bool, error) {
	_, err := GetConnection(ctx, connectionID)
	if err != nil {
		if ConnectionIsGone(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
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

// IsWorkerFromContext returns true if the request is from worker.
func IsWorkerFromContext(ctx context.Context) bool {
	isWorker, ok := ctx.Value(contextKeyIsWorker).(bool)
	return ok && isWorker
}

// Used return true if the request handled by canyon.
func Used(r *http.Request) bool {
	ctx := r.Context()
	if ctx == nil {
		return false
	}
	if _, ok := ctx.Value(contextKeyWorkerSender).(WorkerSender); ok {
		return true
	}
	return IsWorker(r)
}

// UsedFromContext returns true if the request handled by canyon.
func UsedFromContext(ctx context.Context) bool {
	if _, ok := ctx.Value(contextKeyWorkerSender).(WorkerSender); ok {
		return true
	}
	return IsWorkerFromContext(ctx)
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
