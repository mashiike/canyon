package canyon

import (
	"errors"
	"net/http"
)

// MessageAttributeValue is a struct for sqs message attribute.
type MessageAttributeValue struct {
	DataType         string
	StringValue      *string
	BinaryValue      []byte
	BinaryListValues [][]byte
	StringListValues []string
}

// SendOptions is a struct for sending sqs message.
type SendOptions struct {
	// MessageAttributes is a map of sqs message attributes.
	MessageAttributes map[string]MessageAttributeValue

	// MessageGroupID is a message group id for sqs message.
	MessageGroupID *string

	// DelaySeconds is a delay seconds for sqs message.
	DelaySeconds *int32
}

// WorkerSender is a interface for sending sqs message.
// for testing, not for production use.
type WorkerSender interface {
	SendToWorker(r *http.Request, opts *SendOptions) (string, error)
}

// WorkerSenderFunc is a func type for sending sqs message.
// for testing, not for production use.
type WorkerSenderFunc func(*http.Request, *SendOptions) (string, error)

func (f WorkerSenderFunc) SendToWorker(r *http.Request, opts *SendOptions) (string, error) {
	return f(r, opts)
}

func SendToWorker(r *http.Request, opts *SendOptions) (string, error) {
	workerSender := workerSenderFromContext(r.Context())
	if workerSender == nil {
		return "", errors.New("sqs message sender is not set: may be worker or not running with canyon")
	}
	return workerSender.SendToWorker(r, opts)
}
