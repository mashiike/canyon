package canyon

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
)

func TestFakeSQSClient(t *testing.T) {
	var logs bytes.Buffer
	t.Cleanup(func() {
		t.Log("Logs\n", logs.String())
	})
	fakeSQSClient := &fakeSQSClient{
		visibilityTimeout: time.Second,
		logger:            slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	getQueueURLResult, err := fakeSQSClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String("test-queue"),
	})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		require.NoError(t, err, "should get queue url")
		require.Equal(t, "https://sqs.ap-northeast-1.amazonaws.com/123456789012/test-queue", *getQueueURLResult.QueueUrl, "should get queue url")
		sendMessageResult, err := fakeSQSClient.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    getQueueURLResult.QueueUrl,
			MessageBody: aws.String(`{"foo":"bar baz"}`),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"foo": {
					DataType:    aws.String("String"),
					StringValue: aws.String("bar baz"),
				},
				"number": {
					DataType:    aws.String("Number"),
					StringValue: aws.String("123"),
				},
				"binary": {
					DataType:    aws.String("Binary"),
					BinaryValue: []byte("binary"),
				},
			},
		})
		require.NoError(t, err, "should send message")
		require.NotEmpty(t, sendMessageResult.MessageId, "should have message id")
		require.Equal(t, 1, fakeSQSClient.MessageCount(), "should have 1 message in fake sqs client")
	}()
	var receiptHandle string
	go func() {
		defer wg.Done()
		receiveMessageResult, err := fakeSQSClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            getQueueURLResult.QueueUrl,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   3,
			WaitTimeSeconds:     20,
		})
		require.NoError(t, err, "should receive message")
		require.Equal(t, 1, len(receiveMessageResult.Messages), "should have 1 message")
		require.Equal(t, `{"foo":"bar baz"}`, *receiveMessageResult.Messages[0].Body, "should have message body")
		receiptHandle = *receiveMessageResult.Messages[0].ReceiptHandle
	}()
	wg.Wait()
	require.NotEmpty(t, receiptHandle, "should have receipt handle")
	_, err = fakeSQSClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      getQueueURLResult.QueueUrl,
		ReceiptHandle: &receiptHandle,
	})
	require.NoError(t, err, "should delete message")
}

func TestSQSLongPollingService__WithFakeSQS(t *testing.T) {
	var logs bytes.Buffer
	t.Cleanup(func() {
		t.Log("Logs\n", logs.String())
	})
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	fakeSQSClient := &fakeSQSClient{
		visibilityTimeout: time.Second,
		logger:            logger,
	}
	svc := &sqsLongPollingService{
		sqsClient:           fakeSQSClient,
		logger:              logger,
		queueURL:            "https://sqs.ap-northeast-1.amazonaws.com/123456789012/test-queue",
		maxDeleteRetry:      3,
		waitTimeSeconds:     1,
		maxNumberObMessages: 1,
	}

	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(svc.queueURL),
		MessageBody: aws.String(`{"foo":"bar baz"}`),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	receivedMessages := make(chan events.SQSMessage, 1)
	go func() {
		defer wg.Done()
		err := svc.Start(ctx, func(ctx context.Context, event *events.SQSEvent) (*events.SQSEventResponse, error) {
			require.Equal(t, 1, len(event.Records), "should have 1 message")
			require.JSONEq(t, `{"foo":"bar baz"}`, event.Records[0].Body, "should have message body")
			receivedMessages <- event.Records[0]
			close(receivedMessages)
			return &events.SQSEventResponse{}, nil
		})
		require.ErrorIs(t, err, context.Canceled, "should be canceled")
	}()
	output, err := fakeSQSClient.SendMessage(ctx, input)
	require.NoError(t, err, "should send message")
	msg := <-receivedMessages
	require.Equal(t, *output.MessageId, msg.MessageId, "should receive message")
	cancel()
	wg.Wait()
	require.Equal(t, 0, len(fakeSQSClient.messages), "should have no message in fake sqs client")
}

func TestSQSLongPollingService__WithAWS(t *testing.T) {
	queueName := os.Getenv("TEST_SQS_QUEUE_NAME")
	if queueName == "" {
		t.Skip("TEST_SQS_QUEUE_NAME is not set")
	}
	var logs bytes.Buffer
	t.Cleanup(func() {
		t.Log("Logs\n", logs.String())
	})
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	awsCfg, err := config.LoadDefaultConfig(context.Background())
	require.NoError(t, err, "should load aws default config")
	client := sqs.NewFromConfig(awsCfg)
	getQueueUrl, err := client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err, "should get queue url")
	svc := &sqsLongPollingService{
		sqsClient:           client,
		logger:              logger,
		queueURL:            *getQueueUrl.QueueUrl,
		maxDeleteRetry:      3,
		waitTimeSeconds:     1,
		maxNumberObMessages: 1,
	}

	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(svc.queueURL),
		MessageBody: aws.String(`{"foo":"bar baz"}`),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	receivedMessages := make(chan events.SQSMessage, 1)
	go func() {
		defer wg.Done()
		err := svc.Start(ctx, func(ctx context.Context, event *events.SQSEvent) (*events.SQSEventResponse, error) {
			require.Equal(t, 1, len(event.Records), "should have 1 message")
			require.JSONEq(t, `{"foo":"bar baz"}`, event.Records[0].Body, "should have message body")
			receivedMessages <- event.Records[0]
			close(receivedMessages)
			return &events.SQSEventResponse{}, nil
		})
		require.ErrorIs(t, err, context.Canceled, "should be canceled")
	}()
	output, err := client.SendMessage(ctx, input)
	require.NoError(t, err, "should send message")
	msg := <-receivedMessages
	require.Equal(t, *output.MessageId, msg.MessageId, "should receive message")
	cancel()
	wg.Wait()

	getAttributes, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(svc.queueURL),
		AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessages"},
	})
	require.NoError(t, err, "should get queue attributes")
	require.Equal(t, "0", getAttributes.Attributes["ApproximateNumberOfMessages"], "should have no message in queue")
}
