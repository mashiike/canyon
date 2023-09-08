package canyon_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/mashiike/canyon"
	"github.com/mashiike/canyon/canyontest"
	"github.com/stretchr/testify/require"
)

type mockSQSClient struct {
	SendMessageFunc    func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessageFunc func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageFunc  func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
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

func TestE2E_SendToWorkerFailed(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
