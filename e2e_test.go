package canyon_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/mashiike/canyon"
	"github.com/mashiike/canyon/canyontest"
	"github.com/stretchr/testify/require"
)

type mockSQSClient struct {
	SendMessageFunc                  func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessageFunc               func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageFunc                func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibilityBatchFunc func(ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
}

func (c *mockSQSClient) GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	return &sqs.GetQueueAttributesOutput{
		Attributes: map[string]string{
			"VisibilityTimeout": "30",
		},
	}, nil
}
func (c *mockSQSClient) GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://sqs.ap-northeast-1.amazonaws.com/123456789012/canyon-test"),
	}, nil
}

func (c *mockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if c.SendMessageFunc != nil {
		return c.SendMessageFunc(ctx, params, optFns...)
	}
	return &sqs.SendMessageOutput{
		MessageId: aws.String("message-id"),
	}, nil
}

func (c *mockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if c.ReceiveMessageFunc != nil {
		return c.ReceiveMessageFunc(ctx, params, optFns...)
	}
	return &sqs.ReceiveMessageOutput{
		Messages: []types.Message{},
	}, nil
}

func (c *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if c.DeleteMessageFunc != nil {
		return c.DeleteMessageFunc(ctx, params, optFns...)
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (c *mockSQSClient) ChangeMessageVisibilityBatch(ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	if c.ChangeMessageVisibilityBatchFunc != nil {
		return c.ChangeMessageVisibilityBatchFunc(ctx, params, optFns...)
	}
	return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
}

func TestE2E_SendToWorkerFailed(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.True(t, canyon.Used(r), "should be used")
		if !canyon.IsWorker(r) {
			if _, err := canyon.SendToWorker(r, nil); err != nil {
				require.EqualError(t, err, "failed to send sqs message: test")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	r := canyontest.NewRunner(
		h,
		canyon.WithSQSClient(&mockSQSClient{
			SendMessageFunc: func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
				return nil, errors.New("test")
			},
		}),
	)
	defer r.Close()
	resp, err := http.Post(r.URL, "text/plain", nil)
	require.NoError(t, err, "should post")
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode, "should return 500")
}

func TestE2E__HandlerReadBody(t *testing.T) {
	var serverBody, workerBody []byte
	var wg sync.WaitGroup
	wg.Add(1)
	r := canyontest.NewRunner(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var err error
			defer r.Body.Close()
			if !canyon.IsWorker(r) {
				serverBody, err = io.ReadAll(r.Body)
				require.NoError(t, err, "should read body")
				if _, err := canyon.SendToWorker(r, nil); err != nil {
					require.EqualError(t, err, "failed to send sqs message: test")
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusOK)
				return
			}
			workerBody, err = io.ReadAll(r.Body)
			require.NoError(t, err, "should read body")
			w.WriteHeader(http.StatusOK)
			wg.Done()
		}),
	)
	defer r.Close()
	resp, err := http.Post(r.URL, "text/plain", strings.NewReader("hello world"))
	require.NoError(t, err, "should post")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "should return 200")
	wg.Wait()
	require.Equal(t, "hello world", string(serverBody), "should read body")
	require.Equal(t, "hello world", string(workerBody), "should read body")
}

func TestE2E__WithRetryAfterHeader(t *testing.T) {
	callCount := 0
	doneCh := make(chan struct{})
	r := canyontest.NewRunner(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if canyon.IsWorker(r) {
				callCount++
				if callCount == 1 {
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusOK)
				close(doneCh)
				return
			}
			canyon.SendToWorker(r, nil)
			w.WriteHeader(http.StatusOK)
		}),
	)
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequest(http.MethodPost, r.URL, strings.NewReader("test body"))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	select {
	case <-ctx.Done():
		require.Fail(t, "timeout")
	case <-doneCh:
		require.Equal(t, 2, callCount)
	}
}

type AlwaysDeserializeErrorSerializer struct {
	canyon.Serializer
}

func (s *AlwaysDeserializeErrorSerializer) Deserialize(ctx context.Context, message *events.SQSMessage) (*http.Request, error) {
	return nil, errors.New("always deserialize error")
}

type mutexWriter struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (w *mutexWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *mutexWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func TestE2E__DeserializeErrorWithRetryAfter(t *testing.T) {
	logBuf := mutexWriter{
		buf: strings.Builder{},
	}
	defer func() {
		t.Log(logBuf.String())
	}()
	logger := slog.New(slog.NewTextHandler(
		&logBuf,
		&slog.HandlerOptions{
			Level: slog.LevelDebug,
		},
	))
	serializer := canyon.NewRetryAfterSerializer(
		&AlwaysDeserializeErrorSerializer{Serializer: canyon.NewDefaultSerializer()},
		0, 0,
	)
	r := canyontest.NewRunner(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if canyon.IsWorker(r) {
				require.Fail(t, "should not be worker, maybe no deserialize error")
				return
			}
			canyon.SendToWorker(r, nil)
			w.WriteHeader(http.StatusOK)
		}),
		canyon.WithLogger(logger),
		canyon.WithVarbose(),
		canyon.WithSerializer(serializer),
	)
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequest(http.MethodPost, r.URL, strings.NewReader("test body"))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	dlqCh := make(chan struct{})
	go func() {
		var msg json.RawMessage
		err := json.NewDecoder(r.DLQReader()).Decode(&msg)
		t.Log(string(msg))
		if !errors.Is(err, io.ErrClosedPipe) {
			require.NoError(t, err)
		}
		close(dlqCh)
	}()
	select {
	case <-ctx.Done():
		require.Fail(t, "timeout")
	case <-dlqCh:
	}
}

func TestUsed(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.False(t, canyon.Used(r), "should be used")
		w.WriteHeader(http.StatusOK)
	})
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)

	h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.True(t, canyon.Used(r), "should be used")
		w.WriteHeader(http.StatusOK)
	})
	r = httptest.NewRequest(http.MethodPost, "/", nil)
	w = httptest.NewRecorder()
	canyontest.AsWorker(h).ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)

	r = httptest.NewRequest(http.MethodPost, "/", nil)
	w = httptest.NewRecorder()
	canyontest.AsServer(h, canyon.WorkerSenderFunc(
		func(r *http.Request, opts *canyon.SendOptions) (string, error) {
			return "", nil
		},
	)).ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
}
