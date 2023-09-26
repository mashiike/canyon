package canyontest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/mashiike/canyon"
	"github.com/mashiike/canyon/canyontest"
	"github.com/stretchr/testify/require"
)

func TestAsServer(t *testing.T) {
	sh := canyontest.AsServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			require.Equal(t, "/", r.URL.Path)
			require.False(t, canyon.IsWorker(r), "should not be worker")
			msgId, err := canyon.SendToWorker(r, nil)
			require.NoError(t, err, "should send to sqs from server")
			require.Equal(t, "message-id", msgId, "should return message id")
			w.WriteHeader(http.StatusOK)
		}),
		canyon.WorkerSenderFunc(func(r *http.Request, _ canyon.MessageAttributes) (string, error) {
			require.Equal(t, "POST", r.Method)
			require.Equal(t, "/", r.URL.Path)
			return "message-id", nil
		}),
	)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	sh.ServeHTTP(w, req)
	resp := w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode, "should return 200")
}

func TestAsWorker(t *testing.T) {
	sh := canyontest.AsWorker(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			require.Equal(t, "/", r.URL.Path)
			require.True(t, canyon.IsWorker(r), "should be worker")
			_, err := canyon.SendToWorker(r, nil)
			require.Error(t, err, "should can not send to sqs from worker")
			require.Equal(t, canyontest.DummySQSMessage.MessageId, r.Header.Get(canyon.HeaderSQSMessageId), "should embed sqs message id")
			w.WriteHeader(http.StatusOK)
		}),
	)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	sh.ServeHTTP(w, req)
	resp := w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode, "should return 200")
}

func TestAsLambdaFallbak(t *testing.T) {
	var lastSend json.RawMessage
	sh := canyontest.AsLambdaFallback(
		lambda.NewHandler(func(ctx context.Context, event json.RawMessage) error {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/", bytes.NewReader(event))
			require.NoError(t, err, "should create request")
			messageID, err := canyon.SendToWorker(req, nil)
			require.NoError(t, err, "should send to sqs from lambda")
			require.Equal(t, "message-id", messageID, "should return message id")
			return nil
		}),
		canyon.WorkerSenderFunc(func(r *http.Request, _ canyon.MessageAttributes) (string, error) {
			require.Equal(t, "POST", r.Method)
			require.Equal(t, "/", r.URL.Path)
			json.NewDecoder(r.Body).Decode(&lastSend)
			return "message-id", nil
		}),
	)
	_, err := sh.Invoke(context.Background(), []byte(`{"key":"value"}`))
	require.NoError(t, err, "should invoke lambda")
	require.Equal(t, `{"key":"value"}`, string(lastSend), "should send event to sqs")
}
