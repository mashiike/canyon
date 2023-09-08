package canyontest_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

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
		canyon.SQSMessageSenderFunc(func(r *http.Request, _ canyon.MessageAttributes) (string, error) {
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
