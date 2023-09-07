package canyon

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/fujiwara/ridge"
	"github.com/stretchr/testify/require"
)

func TestWorkerHandler__Success(t *testing.T) {
	var logs bytes.Buffer
	t.Cleanup(func() {
		t.Log("Logs\n", logs.String())
	})
	c := defaultRunConfig(nil, "test-queue")
	c.logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	workerHandler := newWorkerHandler(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			l := Logger(r)
			l.Info("hello")
			require.Equal(t, "POST", r.Method)
			require.Equal(t, "/", r.URL.Path)
			require.True(t, IsWorker(r), "should be worker")
			err := r.ParseForm()
			require.NoError(t, err, "should parse form")
			require.Equal(t, "bar baz", r.Form.Get("foo"))
			w.WriteHeader(http.StatusOK)
			_, err = SendToWorker(r, nil)
			require.Error(t, err, "should can not send to sqs from worker")
		}), c,
	)
	sqsEvent := ReadJSON[events.SQSEvent](t, "testdata/sqs_event.json")
	resp, err := workerHandler(context.Background(), sqsEvent)
	require.NoError(t, err, "should handle sqs event")
	require.Equal(t, 0, len(resp.BatchItemFailures), "should have no failures")
}

func TestWorkerHandler__Failed(t *testing.T) {
	var logs bytes.Buffer
	t.Cleanup(func() {
		t.Log("Logs\n", logs.String())
	})
	c := defaultRunConfig(nil, "test-queue")
	c.logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	workerHandler := newWorkerHandler(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			l := Logger(r)
			l.Info("hello")
			require.Equal(t, "POST", r.Method)
			require.Equal(t, "/", r.URL.Path)
			require.True(t, IsWorker(r), "should be worker")
			err := r.ParseForm()
			require.NoError(t, err, "should parse form")
			require.Equal(t, "bar baz", r.Form.Get("foo"))
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "worker failed: this is test")
		}), c,
	)
	sqsEvent := ReadJSON[events.SQSEvent](t, "testdata/sqs_event.json")
	resp, err := workerHandler(context.Background(), sqsEvent)
	require.NoError(t, err, "should handle sqs event")
	require.Equal(t, 1, len(resp.BatchItemFailures), "should have 1 failures")
	require.Equal(t, "00000000-0000-0000-0000-000000000000", resp.BatchItemFailures[0].ItemIdentifier, "should have message id")
}

func TestServerHandler(t *testing.T) {
	var logs bytes.Buffer
	t.Cleanup(func() {
		t.Log("Logs\n", logs.String())
	})
	c := defaultRunConfig(nil, "test-queue")
	c.logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	sqsClient := &fakeSQSClient{
		visibilityTimeout: 30 * time.Second,
		logger:            c.logger,
	}
	WithSQSClient(sqsClient)(c)
	var messageId string
	serverHandler := newServerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l := Logger(r)
		l.Info("hello")
		require.False(t, IsWorker(r), "should not be worker")
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "/", r.URL.Path)
		err := r.ParseForm()
		require.NoError(t, err, "should parse form")
		require.Equal(t, "bar baz", r.Form.Get("foo"))
		messageId, err = SendToWorker(r, map[string]types.MessageAttributeValue{
			"foo": {
				DataType:    aws.String("String"),
				StringValue: aws.String("bar"),
			},
			"number": {
				DataType:    aws.String("Number"),
				StringValue: aws.String("123"),
			},
		})
		require.NoError(t, err, "should send to sqs")
		w.WriteHeader(http.StatusOK)
	}), c)

	req, err := ridge.NewRequest(ReadFile(t, "testdata/http_event.json"))
	resp := httptest.NewRecorder()
	require.NoError(t, err, "should create request")
	serverHandler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code, "should be accepted")
	require.Equal(t, 1, len(sqsClient.messages), "should send 1 message")
	msg, ok := sqsClient.messages[messageId]
	require.True(t, ok, "should have message")
	require.Equal(t, "bar", *msg.MessageAttributes["foo"].StringValue, "should send message with foo")
	require.Equal(t, "123", *msg.MessageAttributes["number"].StringValue, "should send message with number")
}
