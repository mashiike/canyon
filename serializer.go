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
)

type Serializer interface {
	Serialize(ctx context.Context, r *http.Request) (*sqs.SendMessageInput, error)
	Deserialize(ctx context.Context, message *events.SQSMessage) (*http.Request, error)
}

type BackendSerializer interface {
	Serializer
	WithBackend(backend Backend) Serializer
}

type LoggingableSerializer interface {
	Serializer
	WithLogger(logger *slog.Logger) Serializer
}

// jsonSerializableRequest is a request that can be serialized to JSON.
type jsonSerializableRequest struct {
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

// DefaultSerializer is a struct for Serialize and Deserialize http.Request as SQS Message.
type DefaultSerializer struct {
	Backend Backend
	Logger  *slog.Logger
}

func NewDefaultSerializer() *DefaultSerializer {
	return &DefaultSerializer{
		Logger: slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
	}
}

func (s *DefaultSerializer) WithBackend(backend Backend) Serializer {
	cloned := s.Clone()
	cloned.Backend = backend
	return cloned
}

func (s *DefaultSerializer) WithLogger(logger *slog.Logger) Serializer {
	cloned := s.Clone()
	cloned.Logger = logger
	return cloned
}

func (s *DefaultSerializer) Clone() *DefaultSerializer {
	return &DefaultSerializer{
		Backend: s.Backend,
		Logger:  s.Logger,
	}
}

// Serialize does serialize http.Request as SQS Message.
func (s *DefaultSerializer) Serialize(ctx context.Context, r *http.Request) (*sqs.SendMessageInput, error) {
	sr := &jsonSerializableRequest{
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
	msg := &sqs.SendMessageInput{
		MessageBody: aws.String(string(bs)),
	}
	return msg, nil
}

// Deserialize does deserialize SQS Message as http.Request.
func (s *DefaultSerializer) Deserialize(ctx context.Context, message *events.SQSMessage) (*http.Request, error) {
	sr := &jsonSerializableRequest{}
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
