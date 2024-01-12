package canyon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Songmu/flextime"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/fujiwara/ridge"
	"github.com/stretchr/testify/require"
)

func TestInMemorySQSClient(t *testing.T) {
	var logs bytes.Buffer
	t.Cleanup(func() {
		t.Log("Logs\n", logs.String())
	})
	inMemorySQSClient := &inMemorySQSClient{
		visibilityTimeout: time.Second,
		logger:            slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	getQueueURLResult, err := inMemorySQSClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String("test-queue"),
	})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		require.NoError(t, err, "should get queue url")
		require.Equal(t, "https://sqs.ap-northeast-1.amazonaws.com/123456789012/test-queue", *getQueueURLResult.QueueUrl, "should get queue url")
		sendMessageResult, err := inMemorySQSClient.SendMessage(ctx, &sqs.SendMessageInput{
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
		require.Equal(t, 1, inMemorySQSClient.MessageCount(), "should have 1 message in fake sqs client")
	}()
	var receiptHandle string
	go func() {
		defer wg.Done()
		receiveMessageResult, err := inMemorySQSClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
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
	_, err = inMemorySQSClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      getQueueURLResult.QueueUrl,
		ReceiptHandle: &receiptHandle,
	})
	require.NoError(t, err, "should delete message")
}

func TestInMemorySQSClient__DelaySeconds(t *testing.T) {
	restore := flextime.Set(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))
	defer restore()
	var logs bytes.Buffer
	t.Cleanup(func() {
		t.Log("Logs\n", logs.String())
	})
	inMemorySQSClient := &inMemorySQSClient{
		visibilityTimeout: time.Second,
		logger:            slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	getQueueURLResult, err := inMemorySQSClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String("test-queue"),
	})
	require.NoError(t, err, "should get queue url")
	require.Equal(t, "https://sqs.ap-northeast-1.amazonaws.com/123456789012/test-queue", *getQueueURLResult.QueueUrl, "should get queue url")

	setSendMessage := func(delaySeconds int32) string {
		sendMessageResult, err := inMemorySQSClient.SendMessage(ctx, &sqs.SendMessageInput{
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
			DelaySeconds: delaySeconds,
		})
		require.NoError(t, err, "should send message")
		require.NotEmpty(t, sendMessageResult.MessageId, "should have message id")
		return *sendMessageResult.MessageId
	}

	nonDelayMessageID := setSendMessage(0)
	delayMessageID := setSendMessage(900)
	require.Equal(t, 2, inMemorySQSClient.MessageCount(), "should have 1 message in fake sqs client")

	reciveMessage := func() string {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		receiveMessageResult, err := inMemorySQSClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            getQueueURLResult.QueueUrl,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   3,
			WaitTimeSeconds:     20,
		})
		require.NoError(t, err, "should receive message")
		require.Equal(t, 1, len(receiveMessageResult.Messages), "should have 1 message")
		require.Equal(t, `{"foo":"bar baz"}`, *receiveMessageResult.Messages[0].Body, "should have message body")
		receiptHandle := *receiveMessageResult.Messages[0].ReceiptHandle
		messageID := *receiveMessageResult.Messages[0].MessageId
		require.NotEmpty(t, receiptHandle, "should have receipt handle")
		_, err = inMemorySQSClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      getQueueURLResult.QueueUrl,
			ReceiptHandle: &receiptHandle,
		})
		require.NoError(t, err, "should delete message")
		return messageID
	}
	actualFirstMessageID := reciveMessage()
	require.Equal(t, nonDelayMessageID, actualFirstMessageID, "should receive non delay message first")
	flextime.Set(time.Date(2023, 1, 1, 0, 30, 0, 0, time.UTC))
	actualSecondMessageID := reciveMessage()
	require.Equal(t, delayMessageID, actualSecondMessageID, "should receive delay message second")
}

func TestSQSLongPollingService__WithinMemorySQS(t *testing.T) {
	var logs bytes.Buffer
	t.Cleanup(func() {
		t.Log("Logs\n", logs.String())
	})
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	inMemorySQSClient := &inMemorySQSClient{
		visibilityTimeout: time.Second,
		logger:            logger,
	}
	svc := &sqsLongPollingService{
		sqsClient:           inMemorySQSClient,
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
	output, err := inMemorySQSClient.SendMessage(ctx, input)
	require.NoError(t, err, "should send message")
	msg := <-receivedMessages
	require.Equal(t, *output.MessageId, msg.MessageId, "should receive message")
	cancel()
	wg.Wait()
	require.Equal(t, 0, len(inMemorySQSClient.messages), "should have no message in fake sqs client")
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

type mockS3Client struct {
	t                           *testing.T
	PutObjectFunc               func(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	UploadPartFunc              func(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUploadFunc   func(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUploadFunc func(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUploadFunc    func(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	HeadObjectFunc              func(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObjectFunc               func(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

func (c *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if c.PutObjectFunc == nil {
		c.t.Fatal("PutObjectFunc is not set, unexpected call")
	}
	return c.PutObjectFunc(ctx, params, optFns...)
}

func (c *mockS3Client) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if c.UploadPartFunc == nil {
		c.t.Fatal("UploadPartFunc is not set, unexpected call")
	}
	return c.UploadPartFunc(ctx, params, optFns...)
}

func (c *mockS3Client) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	if c.CreateMultipartUploadFunc == nil {
		c.t.Fatal("CreateMultipartUploadFunc is not set, unexpected call")
	}
	return c.CreateMultipartUploadFunc(ctx, params, optFns...)
}

func (c *mockS3Client) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if c.CompleteMultipartUploadFunc == nil {
		c.t.Fatal("CompleteMultipartUploadFunc is not set, unexpected call")
	}
	return c.CompleteMultipartUploadFunc(ctx, params, optFns...)
}

func (c *mockS3Client) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	if c.AbortMultipartUploadFunc == nil {
		c.t.Fatal("AbortMultipartUploadFunc is not set, unexpected call")
	}
	return c.AbortMultipartUploadFunc(ctx, params, optFns...)
}

func (c *mockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if c.HeadObjectFunc == nil {
		c.t.Fatal("HeadObjectFunc is not set, unexpected call")
	}
	return c.HeadObjectFunc(ctx, params, optFns...)
}

func (c *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if c.GetObjectFunc == nil {
		c.t.Fatal("GetObjectFunc is not set, unexpected call")
	}
	return c.GetObjectFunc(ctx, params, optFns...)
}

func TestS3BackendSaveRequestBody(t *testing.T) {
	req, err := ridge.NewRequest(ReadFile(t, "testdata/http_event.json"))
	require.NoError(t, err, "should create request")

	b, err := NewS3Backend("s3://my-bucket/my-prefix")
	require.NoError(t, err, "should create backend")
	var objectKey string
	b.SetS3Client(&mockS3Client{
		t: t,
		PutObjectFunc: func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			require.Equal(t, "my-bucket", *input.Bucket)
			objectKey = *input.Key
			bs, err := io.ReadAll(input.Body)
			require.NoError(t, err, "should read body")
			require.Equal(t, "foo=bar%20baz", string(bs), "should have body")
			return &s3.PutObjectOutput{}, nil
		},
	})
	t.Log("objectKey", objectKey)
	u, err := b.SaveRequestBody(context.Background(), req)
	require.NoError(t, err, "should save request body")
	require.True(t, strings.HasPrefix(objectKey, "my-prefix/"), "should have prefix")
	require.Equal(t, fmt.Sprintf("s3://my-bucket/%s", objectKey), u.String(), "should have s3 url")
}

func TestS3BackendLoadRequestBody(t *testing.T) {
	b, err := NewS3Backend("s3://my-bucket/my-prefix")
	require.NoError(t, err, "should create backend")
	body := "foo=bar%20baz"
	b.SetS3Client(&mockS3Client{
		t: t,
		HeadObjectFunc: func(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			require.Equal(t, "my-bucket", *input.Bucket)
			require.Equal(t, "my-prefix/my-object/data.bin", *input.Key)
			return &s3.HeadObjectOutput{
				ContentLength: aws.Int64(int64(len(body))),
			}, nil
		},
		GetObjectFunc: func(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			require.Equal(t, "my-bucket", *input.Bucket)
			require.Equal(t, "my-prefix/my-object/data.bin", *input.Key)
			return &s3.GetObjectOutput{
				Body:          io.NopCloser(strings.NewReader(body)),
				ContentLength: aws.Int64(int64(len(body))),
			}, nil
		},
	})
	require.NoError(t, err, "should create request")
	actualReader, err := b.LoadRequestBody(context.Background(), &url.URL{
		Scheme: "s3",
		Host:   "my-bucket",
		Path:   "/my-prefix/my-object/data.bin",
	})
	require.NoError(t, err, "should load request body")
	actual, err := io.ReadAll(actualReader)
	require.Equal(t, body, string(actual), "should save request body")
}
