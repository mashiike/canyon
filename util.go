package canyon

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
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

// Set SQS Message headers to Request
func SetSQSMessageHeader(r *http.Request, message events.SQSMessage) *http.Request {
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
	return r
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
func ToMessageAttributes(h http.Header) MessageAttributes {
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

func isS3URL(u *url.URL) bool {
	if u.Scheme != "s3" {
		return false
	}
	if u.Host == "" {
		return false
	}
	return true
}

var preferredExts = []string{
	".json",
	".txt",
	".html",
	".jpeg",
	".png",
	".gif",
	".svg",
}

func getExtension(contentType string) string {
	exts, err := mime.ExtensionsByType(contentType)
	if err != nil {
		return ".bin"
	}
	if len(exts) > 0 {
		for _, preferredExt := range preferredExts {
			for _, ext := range exts {
				if strings.ToLower(ext) == preferredExt {
					return ext
				}
			}
		}
		return exts[0]
	}
	return ".bin"
}
