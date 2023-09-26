package canyon

import (
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// MessageAttributes is a map of sqs message attributes.
type MessageAttributes map[string]types.MessageAttributeValue

// WorkerSender is a interface for sending sqs message.
// for testing, not for production use.
type WorkerSender interface {
	SendToWorker(r *http.Request, attributes MessageAttributes) (string, error)
}

// WorkerSenderFunc is a func type for sending sqs message.
// for testing, not for production use.
type WorkerSenderFunc func(*http.Request, MessageAttributes) (string, error)

func (f WorkerSenderFunc) SendToWorker(r *http.Request, attributes MessageAttributes) (string, error) {
	return f(r, attributes)
}
