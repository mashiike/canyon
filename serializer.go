package canyon

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// JSONSerializableRequest is a request that can be serialized to JSON.
type JSONSerializableRequest struct {
	BackendURL    *string             `json:"backend_url,omitempty"`
	Method        string              `json:"method,omitempty"`
	Header        map[string][]string `json:"header,omitempty"`
	Body          string              `json:"body,omitempty"`
	ContentLength int64               `json:"content_length,omitempty"`
	RemoteAddr    string              `json:"remote_addr,omitempty"`
	Host          string              `json:"host,omitempty"`
	RequestURI    string              `json:"request_uri,omitempty"`
	URL           string              `json:"url,omitempty"`
}

// Serializer is a struct for Serialize and Deserialize http.Request as SQS Message.
type Serializer struct {
	Backend Backend
	Logger  *slog.Logger
}

func NewSerializer(backend Backend) *Serializer {
	return &Serializer{
		Backend: backend,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
	}
}

func (s *Serializer) SetLogger(logger *slog.Logger) {
	s.Logger = logger
}

// Serialize does serialize http.Request as SQS Message.
func (s *Serializer) Serialize(ctx context.Context, r *http.Request) (*events.SQSMessage, error) {
	sr := &JSONSerializableRequest{
		Method:        r.Method,
		Header:        r.Header,
		ContentLength: r.ContentLength,
		RemoteAddr:    r.RemoteAddr,
		Host:          r.Host,
		RequestURI:    r.RequestURI,
		URL:           r.URL.String(),
	}
	if s.Backend == nil || r.ContentLength == 0 {
		bs, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body: %w", err)
		}
		sr.Body = base64.StdEncoding.EncodeToString(bs)
	} else {
		backendURL, err := s.Backend.SaveRequestBody(ctx, r)
		if err != nil {
			return nil, fmt.Errorf("failed to save request body: %w", err)
		}
		slog.InfoContext(ctx, "saved request body", "uploaded_url", backendURL.String(), "request_uri", r.RequestURI)
		sr.BackendURL = aws.String(backendURL.String())
	}
	bs, err := json.Marshal(sr)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}
	msg := &events.SQSMessage{
		Body: string(bs),
	}
	return msg, nil
}

// Deserialize does deserialize SQS Message as http.Request.
func (s *Serializer) Deserialize(ctx context.Context, message events.SQSMessage) (*http.Request, error) {
	sr := &JSONSerializableRequest{}
	if err := json.Unmarshal([]byte(message.Body), sr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal request: %w", err)
	}
	r, err := http.NewRequest(sr.Method, sr.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	r.Header = sr.Header
	r.ContentLength = sr.ContentLength
	r.RemoteAddr = sr.RemoteAddr
	r.Host = sr.Host
	r.RequestURI = sr.RequestURI
	if sr.BackendURL != nil {
		if s.Backend == nil {
			return nil, errors.New("backend is not set")
		}
		urlObj, err := parseURL(*sr.BackendURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse backend url: %w", err)
		}
		body, err := s.Backend.LoadRequestBody(ctx, urlObj)
		if err != nil {
			return nil, fmt.Errorf("failed to load request body: %w", err)
		}
		r.Body = body
	} else {
		bs, err := base64.StdEncoding.DecodeString(sr.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to decode request body: %w", err)
		}
		r.Body = io.NopCloser(bytes.NewReader(bs))

	}
	r = SetSQSMessageHeader(r, message)
	return r, nil
}

// NewSendMessageInput does create SendMessageInput from http.Request.
func (s *Serializer) NewSendMessageInput(ctx context.Context, sqsQueueURL string, r *http.Request, messageAttrs map[string]types.MessageAttributeValue) (*sqs.SendMessageInput, error) {
	msg, err := s.Serialize(ctx, r)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize request: %w", err)
	}
	params := &sqs.SendMessageInput{
		MessageBody:       aws.String(msg.Body),
		QueueUrl:          aws.String(sqsQueueURL),
		MessageAttributes: messageAttrs,
	}
	return params, nil
}
