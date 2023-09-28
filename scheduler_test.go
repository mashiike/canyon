package canyon

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
)

func TestInMemoryScheduler(t *testing.T) {
	var logs bytes.Buffer
	t.Cleanup(func() {
		t.Log("Logs\n", logs.String())
	})
	sqsClient := &inMemorySQSClient{
		visibilityTimeout: time.Second,
		logger:            slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	getQueueURLResult, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String("test-queue"),
	})
	require.NoError(t, err)
	schedler := NewInMemoryScheduler(func() SQSClient {
		return sqsClient
	})
	err = schedler.RegisterSchedule(ctx, &sqs.SendMessageInput{
		QueueUrl:     getQueueURLResult.QueueUrl,
		MessageBody:  aws.String("test"),
		DelaySeconds: 1,
	})
	require.NoError(t, err)
}
