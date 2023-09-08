package canyon

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

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

// Backend is interface for storing and loading data.
type Backend interface {
	// Save does store data.
	// If data is stored successfully, return URL of stored data.
	// If data is not stored successfully, return error.
	SaveRequestBody(context.Context, *http.Request) (*url.URL, error)

	// Load does load data from URL.
	// If data is loaded successfully, return data.
	// If data is not loaded successfully, return error.
	LoadRequestBody(context.Context, *url.URL) (io.ReadCloser, error)
}

// Serializer is a struct for Serialize and Deserialize http.Request as SQS Message.
type Serializer struct {
	Backend Backend
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
	if s.Backend == nil {
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
		urlObj, err := url.Parse(*sr.BackendURL)
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
	r.Header.Set(HeaderSQSMessageId, message.MessageId)
	r.Header.Set(HeaderSQSEventSource, message.EventSource)
	r.Header.Set(HeaderSQSEventSourceArn, message.EventSourceARN)
	r.Header.Set(HeaderSQSAwsRegionHeader, message.AWSRegion)
	for k, v := range message.Attributes {
		r.Header.Set(HeaderSQSAttribute(k), v)
	}
	for k, v := range message.MessageAttributes {
		parts := strings.SplitN(v.DataType, ".", 2)
		headerName := HeaderSQSMessageAttribute(k, parts[0])
		if v.StringValue != nil {
			switch parts[0] {
			case "String", "Number":
				r.Header.Add(headerName, *v.StringValue)
			default:
				r.Header.Add(headerName, base64.StdEncoding.EncodeToString([]byte(*v.StringValue)))
			}
		}
		for _, s := range v.StringListValues {
			switch parts[0] {
			case "String", "Number":
				r.Header.Add(headerName, s)
			default:
				r.Header.Add(headerName, base64.StdEncoding.EncodeToString([]byte(s)))
			}
		}
		if v.BinaryValue != nil {
			switch parts[0] {
			case "String", "Number":
				r.Header.Add(headerName, string(v.BinaryValue))
			default:
				r.Header.Add(headerName, base64.StdEncoding.EncodeToString(v.BinaryValue))
			}
		}
		for _, b := range v.BinaryListValues {
			switch parts[0] {
			case "String", "Number":
				r.Header.Add(headerName, string(b))
			default:
				r.Header.Add(headerName, base64.StdEncoding.EncodeToString(b))
			}
		}
	}
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
