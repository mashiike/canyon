package canyon

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/fujiwara/ridge"
	"github.com/stretchr/testify/require"
)

type mockBackend struct{}

func (b *mockBackend) SaveRequestBody(_ context.Context, r *http.Request) (*url.URL, error) {
	return &url.URL{
		Scheme: "file",
		Path:   "/tmp/canyon_test",
	}, nil
}

func (b *mockBackend) LoadRequestBody(_ context.Context, _ *url.URL) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("hello=world")), nil
}

var updateFlag = flag.Bool("update", false, "update golden files")

func TestDefaultSerializerSerialize__NonBackend(t *testing.T) {
	req, err := ridge.NewRequest(ReadFile(t, "testdata/http_event.json"))
	require.NoError(t, err, "should create request")

	serializer := NewDefaultSerializer()
	serialized, err := serializer.Serialize(context.Background(), req)
	require.NoError(t, err, "should serialize request")
	fixturePath := "testdata/serialized_http_request.json"
	if *updateFlag {
		var dest bytes.Buffer
		err := json.Indent(&dest, []byte(*serialized.MessageBody), "", "  ")
		require.NoError(t, err, "should indent json")
		err = os.WriteFile(fixturePath, dest.Bytes(), 0644)
		require.NoError(t, err, "should write file")
	}
	require.JSONEq(t, string(ReadFile(t, fixturePath)), string(*serialized.MessageBody), "same as expected serialized request")
}

func TestDefaultSerializerSerialize__MockBackend(t *testing.T) {
	req, err := ridge.NewRequest(ReadFile(t, "testdata/http_event.json"))
	require.NoError(t, err, "should create request")

	serializer := NewDefaultSerializer().WithBackend(&mockBackend{})
	serialized, err := serializer.Serialize(context.Background(), req)
	require.NoError(t, err, "should serialize request")

	fixturePath := "testdata/serialized_http_request_with_mock_backend.json"
	if *updateFlag {
		var dest bytes.Buffer
		err := json.Indent(&dest, []byte(*serialized.MessageBody), "", "  ")
		require.NoError(t, err, "should indent json")
		err = os.WriteFile(fixturePath, dest.Bytes(), 0644)
		require.NoError(t, err, "should write file")
	}
	require.JSONEq(t, string(ReadFile(t, fixturePath)), string(*serialized.MessageBody), "same as expected serialized request")
}

func TestDefaultSerializerDesirialize__NonBackend(t *testing.T) {
	sqsEvent := ReadJSON[events.SQSEvent](t, "testdata/sqs_event.json")
	message := sqsEvent.Records[0]

	serializer := NewDefaultSerializer()
	req, err := serializer.Deserialize(context.Background(), &message)
	require.NoError(t, err, "should deserialize request")

	require.Equal(t, "POST", req.Method, "should be POST")
	require.Equal(t, "abcdefg.execute-api.ap-northeast-1.amazonaws.com", req.Host, "should be abcdefg.execute-api.ap-northeast-1.amazonaws.com")
	require.Equal(t, "/", req.URL.Path, "should be /")
	require.Equal(t, "203.0.113.1", req.RemoteAddr, "should be 203.0.113.1")

	expectedHeader := http.Header{
		"Accept":               {"*/*"},
		"Content-Length":       {"13"},
		"Content-Type":         {"application/x-www-form-urlencoded"},
		"User-Agent":           {"curl/7.68.0"},
		"X-Amzn-Trace-Id":      {"Root=1-5e723db7-6077c85e0d781094f0c83e24"},
		"X-Forwarded-For":      {"203.0.113.1"},
		"X-Forwarded-Port":     {"443"},
		"X-Forwarded-Proto":    {"https"},
		"Sqs-Message-Id":       {"00000000-0000-0000-0000-000000000000"},
		"Sqs-Event-Source":     {"aws:sqs"},
		"Sqs-Event-Source-Arn": {"arn:aws:sqs:ap-northeast-1:123456789012:sqs-queue"},
		"Sqs-Aws-Region":       {"ap-northeast-1"},
		"Sqs-Attribute-Approximate-First-Receive-Timestamp": {"1693993542501"},
		"Sqs-Attribute-Approximate-Receive-Count":           {"3"},
		"Sqs-Attribute-Sender-Id":                           {"AROAIWPX5BD2BHG722MW4:sender"},
		"Sqs-Attribute-Sent-Timestamp":                      {"1693993542490"},
		"Sqs-Message-Attribute-String-Attribute1":           {"AttributeValue1"},
		"Sqs-Message-Attribute-Number-Attribute2":           {"123", "1", "0"},
		"Sqs-Message-Attribute-Binary-Attribute3":           {"YWJj", "MTIz", "MTEwMA==", "MA==", "MQ==", "MA=="},
		"Sqs-Message-Attribute-String-Sender":               {"canyon"},
	}
	require.EqualValues(t, expectedHeader, req.Header, "should have header")
	err = req.ParseForm()
	require.NoError(t, err, "should parse form")
	require.Equal(t, "bar baz", req.Form.Get("foo"), "should be foo=bar%20baz")
}

func TestDefaultSerializerDesirialize__MockBackend(t *testing.T) {
	sqsEvent := ReadJSON[events.SQSEvent](t, "testdata/sqs_event_with_mock_backend.json")
	message := sqsEvent.Records[0]

	serializer := NewDefaultSerializer().WithBackend(&mockBackend{})
	req, err := serializer.Deserialize(context.Background(), &message)
	require.NoError(t, err, "should deserialize request")

	require.Equal(t, "POST", req.Method, "should be POST")
	require.Equal(t, "abcdefg.execute-api.ap-northeast-1.amazonaws.com", req.Host, "should be abcdefg.execute-api.ap-northeast-1.amazonaws.com")
	require.Equal(t, "/", req.URL.Path, "should be /")
	require.Equal(t, "203.0.113.1", req.RemoteAddr, "should be 203.0.113.1")
	require.Equal(t, "00000000-0000-0000-0000-000000000000", req.Header.Get(HeaderSQSMessageID), "should be include sqs message id")
	require.Equal(t, "aws:sqs", req.Header.Get(HeaderSQSEventSource), "should be include sqs event source")
	require.Equal(t, "arn:aws:sqs:ap-northeast-1:123456789012:sqs-queue", req.Header.Get(HeaderSQSEventSourceArn), "should be include sqs event source arn")
	err = req.ParseForm()
	require.NoError(t, err, "should parse form")
	require.Equal(t, "world", req.Form.Get("hello"), "should be foo=bar%20baz")
}
