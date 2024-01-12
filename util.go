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
	"net/textproto"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var commonInitialisms = map[string]bool{
	"ACL":   true,
	"API":   true,
	"ASCII": true,
	"AWS":   true,
	"CPU":   true,
	"CSS":   true,
	"DNS":   true,
	"EOF":   true,
	"GUID":  true,
	"HTML":  true,
	"HTTP":  true,
	"HTTPS": true,
	"ID":    true,
	"IP":    true,
	"JSON":  true,
	"LHS":   true,
	"QPS":   true,
	"RAM":   true,
	"RHS":   true,
	"RPC":   true,
	"SLA":   true,
	"SMTP":  true,
	"SQL":   true,
	"SSH":   true,
	"TCP":   true,
	"TLS":   true,
	"TTL":   true,
	"UDP":   true,
	"UI":    true,
	"UID":   true,
	"UUID":  true,
	"URI":   true,
	"URL":   true,
	"UTF8":  true,
	"VM":    true,
	"XML":   true,
	"XSRF":  true,
	"XSS":   true,
}

// camelCaseToKebabCase converts camelCase to kebab-case.
// for example: ApproximateFirstReceiveTimestamp -> Approximate-First-Receive-Timestamp
func camelCaseToKebabCase(s string) string {
	var buf bytes.Buffer
	for i, r := range s {
		if i == 0 {
			buf.WriteRune(unicode.ToLower(r))
		} else if unicode.IsUpper(r) {
			if !unicode.IsUpper(rune(s[i-1])) || (i+1 < len(s) && !unicode.IsUpper(rune(s[i+1]))) {
				buf.WriteByte('-')
			}
			buf.WriteRune(unicode.ToLower(r))
		} else {
			buf.WriteRune(r)
		}
	}
	return textproto.CanonicalMIMEHeaderKey(buf.String())
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
	camelCase := buf.String()
	caser := cases.Title(language.English)
	for initialism := range commonInitialisms {
		camelCase = strings.ReplaceAll(camelCase, caser.String(strings.ToLower(initialism)), initialism)
	}
	return camelCase
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
	HeaderSQSMessageID              = "Sqs-Message-Id"
	HeaderSQSMessageGroupID         = "Sqs-Message-Group-Id"
	HeaderSQSMessageDelaySeconds    = "Sqs-Message-Delay-Seconds"
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
func SetSQSMessageHeader(r *http.Request, message *events.SQSMessage) *http.Request {
	r.Header.Set(HeaderSQSMessageID, message.MessageId)
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

const (
	HeaderAPIGatewayWebsocketConnectionID = "Api-Gateway-Websocket-Connection-Id"
	HeaderAPIGatewayWebsocketRouteKey     = "Api-Gateway-Websocket-Route-Key"
	HeaderAPIGatewayWebsocketAPIID        = "Api-Gateway-Websocket-Api-Id"
	HeaderAPIGatewayWebsocketStage        = "Api-Gateway-Websocket-Stage"
)

func SetAPIGatewayWebsocketProxyHeader(r *http.Request, reqCtx *events.APIGatewayWebsocketProxyRequestContext) *http.Request {
	r.Header.Set(HeaderAPIGatewayWebsocketConnectionID, reqCtx.ConnectionID)
	r.Header.Set(HeaderAPIGatewayWebsocketRouteKey, reqCtx.RouteKey)
	r.Header.Set(HeaderAPIGatewayWebsocketAPIID, reqCtx.APIID)
	r.Header.Set(HeaderAPIGatewayWebsocketStage, reqCtx.Stage)
	return r
}

func WebsocketRouteKey(r *http.Request) string {
	return r.Header.Get(HeaderAPIGatewayWebsocketRouteKey)
}

func WebsocketConnectionID(r *http.Request) string {
	return r.Header.Get(HeaderAPIGatewayWebsocketConnectionID)
}

// IsWebsocket returns true if the request is from websocket proxy.
func IsWebsocket(r *http.Request) bool {
	return WebsocketRouteKey(r) != "" && WebsocketConnectionID(r) != ""
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
func ToMessageAttributes(h http.Header) map[string]MessageAttributeValue {
	m := make(map[string]MessageAttributeValue, len(h))
	for k, v := range h {
		if len(v) == 0 {
			continue
		}
		k := kebabCaseToCamelCase(k)
		if len(v) == 1 {
			m[k] = MessageAttributeValue{
				DataType:    "String",
				StringValue: aws.String(v[0]),
			}
			continue
		}
		sl := make([]string, len(v))
		copy(sl, v)
		m[k] = MessageAttributeValue{
			DataType:         "String",
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

func parseURL(urlStr string) (*url.URL, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		u.Scheme = "file"
	}
	if u.Scheme == "file" {
		if u.Host != "" {
			return nil, fmt.Errorf("invalid file url: %s", urlStr)
		}
		if u.Path == "" {
			return nil, fmt.Errorf("invalid file url: %s", urlStr)
		}
	}
	return u, nil
}

func isLambda() bool {
	return strings.HasPrefix(os.Getenv("AWS_EXECUTION_ENV"), "AWS_Lambda") || os.Getenv("AWS_LAMBDA_RUNTIME_API") != ""
}

// sqsQueueURLToArn converts SQS Queue URL to SQS Queue ARN.
// example: https://sqs.ap-northeast-1.amazonaws.com/123456789012/test-queue => arn:aws:sqs:ap-northeast-1:123456789012:test-queue
func sqsQueueURLToArn(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if u.Scheme != "https" {
		return "", fmt.Errorf("invalid scheme: %s", u.Scheme)
	}
	if !strings.HasPrefix(u.Host, "sqs.") || !strings.HasSuffix(u.Host, ".amazonaws.com") {
		return "", fmt.Errorf("invalid host: %s", u.Host)
	}
	region := strings.TrimSuffix(strings.TrimPrefix(u.Host, "sqs."), ".amazonaws.com")
	parts := strings.Split(u.Path, "/")
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid path: %s", u.Path)
	}
	arnStr := arn.ARN{
		Partition: "aws",
		Service:   "sqs",
		Region:    region,
		AccountID: parts[1],
		Resource:  parts[2],
	}.String()
	return arnStr, nil
}
