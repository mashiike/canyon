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
	"math/rand"
	"net/http"
	"strconv"

	"github.com/Songmu/flextime"
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
	Backend              Backend
	Logger               *slog.Logger
	BaseRetrySeconds     int32
	JitterOfRetrySeconds int32
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
	if str := r.Header.Get(HeaderSQSMessageGroupID); str != "" {
		msg.MessageGroupId = aws.String(str)
	}
	if str := r.Header.Get(HeaderSQSMessageDelaySeconds); str != "" {
		delaysSeconds, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse delay seconds: %w", err)
		}
		msg.DelaySeconds = int32(delaysSeconds)
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

type RetryAfterSerializer struct {
	Serializer
	BaseRetrySeconds     int32
	JitterOfRetrySeconds int32
	rand                 *rand.Rand
}

func NewRetryAfterSerializer(serializer Serializer, base int32, jitter int32) *RetryAfterSerializer {
	return &RetryAfterSerializer{
		Serializer:           serializer,
		BaseRetrySeconds:     base,
		JitterOfRetrySeconds: jitter,
		rand:                 rand.New(rand.NewSource(flextime.Now().UnixNano())),
	}
}

func (s *RetryAfterSerializer) Serialize(ctx context.Context, r *http.Request) (*sqs.SendMessageInput, error) {
	return s.Serializer.Serialize(ctx, r)
}

func (s *RetryAfterSerializer) Deserialize(ctx context.Context, message *events.SQSMessage) (*http.Request, error) {
	r, err := s.Serializer.Deserialize(ctx, message)
	if err == nil {
		return r, nil
	}
	retryAfter := s.BaseRetrySeconds
	if s.JitterOfRetrySeconds > 0 {
		retryAfter += s.rand.Int31n(s.JitterOfRetrySeconds)
	}
	return nil, WrapRetryAfter(err, retryAfter)
}

func (s *RetryAfterSerializer) Clone() *RetryAfterSerializer {
	return &RetryAfterSerializer{
		Serializer:           s.Serializer,
		BaseRetrySeconds:     s.BaseRetrySeconds,
		JitterOfRetrySeconds: s.JitterOfRetrySeconds,
		rand:                 s.rand,
	}
}

func (s *RetryAfterSerializer) WithBackend(backend Backend) Serializer {
	if bs, ok := s.Serializer.(BackendSerializer); ok {
		cloned := s.Clone()
		cloned.Serializer = bs.WithBackend(backend)
		return cloned
	}
	return s
}

func (s *RetryAfterSerializer) WithLogger(logger *slog.Logger) Serializer {
	if ls, ok := s.Serializer.(LoggingableSerializer); ok {
		cloned := s.Clone()
		cloned.Serializer = ls.WithLogger(logger)
		return cloned
	}
	return s
}
