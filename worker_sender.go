package canyon

import (
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
