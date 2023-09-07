package canyon

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// camelCaseToKebabCase converts camelCase to kebab-case.
// for example: ApproximateFirstReceiveTimestamp -> Approximate-First-Receive-Timestamp
func camelCaseToKebabCase(s string) string {
	var buf bytes.Buffer
	for i, r := range s {
		if i > 0 && 'A' <= r && r <= 'Z' {
			buf.WriteByte('-')
		}
		buf.WriteRune(r)
	}
	return buf.String()
}

func kebabCaseToCamelCase(s string) string {
	var buf bytes.Buffer
	for i, r := range s {
		if i > 0 && r == '-' {
			continue
		}
		if i > 0 && 'a' <= r && r <= 'z' && s[i-1] == '-' {
			buf.WriteRune(r - 32)
			continue
		}
		buf.WriteRune(r)
	}
	return buf.String()
}

func coalesce(strs ...*string) string {
	for _, str := range strs {
		if str != nil {
			return *str
		}
	}
	return ""
}

// NewSendMessageInputWithRequest creates SendMessageInput from http.Request.
func NewSendMessageInputWithRequest(sqsQueueURL string, r *http.Request, messageAttrs map[string]types.MessageAttributeValue) (*sqs.SendMessageInput, error) {
	serialized, err := NewJSONSerializableRequest(r)
	if err != nil {
		return nil, err
	}
	bs, err := json.Marshal(serialized)
	if err != nil {
		return nil, err
	}
	return &sqs.SendMessageInput{
		QueueUrl:          aws.String(sqsQueueURL),
		MessageBody:       aws.String(string(bs)),
		MessageAttributes: messageAttrs,
	}, nil
}

// this headers are request headers, when run on worker.
const (
	HeaderSQSMessageId              = "Sqs-Message-Id"
	HeaderSQSEventSource            = "Sqs-Event-Source"
	HeaderSQSEventSourceArn         = "Sqs-Event-Source-Arn"
	HeaderSQSAwsRegionHeader        = "Sqs-Aws-Region"
	HeaderPrefixSQSAttribute        = "Sqs-Attribute-"
	HeaderPrefixSQSMessageAttribute = "Sqs-Message-Attribute-"
)

// HeaderSQSAttribute returns header name for SQS attribute, when run on worker
func HeaderSQSAttribute(name string) string {
	return HeaderPrefixSQSAttribute + camelCaseToKebabCase(name)
}

// HeaderSQSMessageAttribute returns header name for SQS message attribute, when run on worker
func HeaderSQSMessageAttribute(name, dataType string) string {
	return HeaderPrefixSQSMessageAttribute + dataType + "-" + camelCaseToKebabCase(name)
}

// NewSQSMessageFromRequest creates SQSMessage from http.Request.
func NewRequestWithSQSMessage(sqsMessage events.SQSMessage) (*http.Request, error) {
	var serialized JSONSerializableRequest
	err := json.Unmarshal([]byte(sqsMessage.Body), &serialized)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal sqs message body: %w", err)
	}
	r, err := serialized.Desirialize()
	if err != nil {
		return nil, fmt.Errorf("failed to desirialize request: %w", err)
	}

	r.Header.Set(HeaderSQSMessageId, sqsMessage.MessageId)
	r.Header.Set(HeaderSQSEventSource, sqsMessage.EventSource)
	r.Header.Set(HeaderSQSEventSourceArn, sqsMessage.EventSourceARN)
	r.Header.Set(HeaderSQSAwsRegionHeader, sqsMessage.AWSRegion)
	for k, v := range sqsMessage.Attributes {
		r.Header.Set(HeaderSQSAttribute(k), v)
	}
	for k, v := range sqsMessage.MessageAttributes {
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

var randomReader = rand.New(rand.NewSource(time.Now().UnixNano()))

func randomBytes(n int) []byte {
	bs := make([]byte, n)
	_, err := io.ReadFull(randomReader, bs)
	if err != nil {
		panic(err)
	}
	return bs
}

func md5Digest(s string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(s)))
}

// ToMessageAttributes converts http.Header to SQS MessageAttributes.
func ToMessageAttributes(h http.Header) map[string]types.MessageAttributeValue {
	m := make(map[string]types.MessageAttributeValue, len(h))
	for k, v := range h {
		if len(v) == 0 {
			continue
		}
		k := kebabCaseToCamelCase(k)
		if len(v) == 1 {
			m[k] = types.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(v[0]),
			}
			continue
		}
		sl := make([]string, len(v))
		for i, s := range v {
			sl[i] = s
		}
		m[k] = types.MessageAttributeValue{
			DataType:         aws.String("String"),
			StringListValues: sl,
		}
	}
	return m
}
