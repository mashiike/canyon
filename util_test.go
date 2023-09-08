package canyon

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/stretchr/testify/require"
)

func ReadFile(t *testing.T, path string) []byte {
	bs, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bs
}

func ReadJSON[T any](t *testing.T, path string) *T {
	var v T
	err := json.Unmarshal(ReadFile(t, path), &v)
	if err != nil {
		t.Fatal(err)
	}
	return &v
}

func TestCamelCaseToKebabCase(t *testing.T) {
	require.Equal(t, "Foo-Bar-Baz", camelCaseToKebabCase("FooBarBaz"))
	require.Equal(t, "foo-Bar-Baz", camelCaseToKebabCase("fooBarBaz"))
	require.Equal(t, "foo-bar-baz", camelCaseToKebabCase("foo-bar-baz"))
	require.Equal(t, "foo_bar_baz", camelCaseToKebabCase("foo_bar_baz"))
	require.Equal(t, "foo bar baz", camelCaseToKebabCase("foo bar baz"))
}

func TestKebabCaseToCamelCase(t *testing.T) {
	require.Equal(t, "FooBarBaz", kebabCaseToCamelCase("Foo-Bar-Baz"))
	require.Equal(t, "fooBarBaz", kebabCaseToCamelCase("foo-Bar-Baz"))
	require.Equal(t, "fooBarBaz", kebabCaseToCamelCase("foo-bar-baz"))
	require.Equal(t, "foo_bar_baz", kebabCaseToCamelCase("foo_bar_baz"))
	require.Equal(t, "foo bar baz", kebabCaseToCamelCase("foo bar baz"))
}

func TestNewRequestWithSQSMessages(t *testing.T) {
	sqsEvent := ReadJSON[events.SQSEvent](t, "testdata/sqs_event.json")
	req, err := NewRequestWithSQSMessage(sqsEvent.Records[0])
	require.NoError(t, err, "should create request")
	bs, err := io.ReadAll(req.Body)

	require.NoError(t, err, "should read request body")
	require.EqualValues(t, "foo=bar%20baz", string(bs), "should have message body")

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
	}
	require.EqualValues(t, expectedHeader, req.Header, "should have header")

	require.EqualValues(t, "POST", req.Method, "should have method")
	require.EqualValues(t, "/", req.URL.Path, "should have path")
}

func TestRundomBytes(t *testing.T) {
	bs := randomBytes(32)
	require.Equal(t, 32, len(bs), "should have 32 bytes")
}

func TestGetExtension(t *testing.T) {
	cases := [][]string{
		{"application/json", ".json"},
		{"application/json; charset=utf-8", ".json"},
		{"application/json;charset=utf-8", ".json"},
		{"application/json; charset=utf-8; foo=bar", ".json"},
		{"image/png", ".png"},
		{"image/gif", ".gif"},
		{"image/jpeg", ".jpeg"},
		{"image/svg+xml", ".svg"},
		{"image/webp", ".webp"},
		{"text/html", ".html"},
		{"text/css", ".css"},
		{"text/javascript", ".js"},
		{"text/plain", ".txt"},
		{"", ".bin"},
	}
	for _, c := range cases {
		require.Equal(t, c[1], getExtension(c[0]), fmt.Sprintf("should get extension `%s`->`%s`", c[0], c[1]))
	}
}
