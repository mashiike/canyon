package canyon

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

type SQSClient interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

type S3Client interface {
	manager.UploadAPIClient
	manager.DownloadAPIClient
	s3.HeadObjectAPIClient
}

type sqsLongPollingService struct {
	sqsClient           SQSClient
	queueURL            string
	maxNumberObMessages int32
	waitTimeSeconds     int32
	maxDeleteRetry      int
	logger              *slog.Logger
}

type sqsEventLambdaHandlerFunc func(context.Context, *events.SQSEvent) (*events.SQSEventResponse, error)

func (svc *sqsLongPollingService) Start(ctx context.Context, fn sqsEventLambdaHandlerFunc) error {
	if svc.maxNumberObMessages < 0 {
		svc.maxNumberObMessages = 1
	}
	if svc.waitTimeSeconds <= 0 {
		svc.waitTimeSeconds = 20
	}
	if svc.maxDeleteRetry <= 0 {
		svc.maxDeleteRetry = 3
	}
	if svc.logger == nil {
		svc.logger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	}
	urlObj, err := url.Parse(svc.queueURL)
	if err != nil {
		return fmt.Errorf("invalid queue url: %w", err)
	}
	getAttributes, err := svc.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(svc.queueURL),
		AttributeNames: []types.QueueAttributeName{"VisibilityTimeout"},
	})
	if err != nil {
		return fmt.Errorf("can not get queue attributes: %w", err)
	}
	visibilityTimeoutStr, ok := getAttributes.Attributes["VisibilityTimeout"]
	var visibilityTimeout int64
	if !ok {
		visibilityTimeout = 30
	} else {
		visibilityTimeout, err = strconv.ParseInt(visibilityTimeoutStr, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid visibility timeout: %w", err)
		}
	}
	arnObj, err := convertQueueURLToARN(urlObj)
	if err != nil {
		return fmt.Errorf("can not convert queue url to arn: %w", err)
	}
	svc.logger.DebugContext(ctx, "start sqs long polling", "queue_url", svc.queueURL, "max_number_of_messages", svc.maxNumberObMessages, "wait_time_seconds", svc.waitTimeSeconds, "visibility_timeout", visibilityTimeout)
	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled || ctx.Err() == context.DeadlineExceeded {
				return context.Cause(ctx)
			}
		default:
		}
		input := &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(svc.queueURL),
			MaxNumberOfMessages:   svc.maxNumberObMessages,
			WaitTimeSeconds:       svc.waitTimeSeconds,
			VisibilityTimeout:     int32(visibilityTimeout),
			AttributeNames:        []types.QueueAttributeName{"All"},
			MessageAttributeNames: []string{"All"},
		}
		svc.logger.DebugContext(ctx, "receive message from sqs queue", "queue_url", *input.QueueUrl, "max_number_of_messages", input.MaxNumberOfMessages, "wait_time_seconds", input.WaitTimeSeconds, "visibility_timeout", input.VisibilityTimeout, "attribute_names", input.AttributeNames, "message_attribute_names", input.MessageAttributeNames)
		output, err := svc.sqsClient.ReceiveMessage(ctx, input)
		if err != nil {
			return err
		}
		if len(output.Messages) == 0 {
			svc.logger.DebugContext(ctx, "no message received from sqs queue", "queue_url", *input.QueueUrl)
			continue
		}
		svc.logger.DebugContext(ctx, "received message from sqs queue", "queue_url", *input.QueueUrl, "messages_count", len(output.Messages))
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(visibilityTimeout)*time.Second)
		err = func() error {
			defer cancel()
			sqsEvent := &events.SQSEvent{
				Records: make([]events.SQSMessage, len(output.Messages)),
			}
			for i, msg := range output.Messages {
				sqsEvent.Records[i] = convertMessageToEventRecord(arnObj, &msg)
			}
			resp, err := fn(timeoutCtx, sqsEvent)
			if err != nil {
				return err
			}
			isFailure := make(map[string]bool, len(output.Messages))
			if resp != nil {
				for _, record := range resp.BatchItemFailures {
					isFailure[record.ItemIdentifier] = true
				}
			}
			var errMu sync.Mutex
			var errs []error
			var wg sync.WaitGroup
			for _, msg := range output.Messages {
				if isFailure[*msg.MessageId] {
					svc.logger.DebugContext(ctx, "skip delete message from sqs queue", "queue_url", *input.QueueUrl, "message_id", *msg.MessageId, "reason", "failure")
					continue
				}
				wg.Add(1)
				go func(msg types.Message) {
					defer wg.Done()
					_, err := svc.sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
						ReceiptHandle: msg.ReceiptHandle,
						QueueUrl:      aws.String(svc.queueURL),
					})
					if err != nil {
						svc.logger.WarnContext(ctx, "can not delete message from sqs queue", "queue_url", svc.queueURL, "message_id", *msg.MessageId, "error", err)
						for i := 1; i <= svc.maxDeleteRetry; i++ {
							svc.logger.InfoContext(ctx, "retry to delete message from sqs queue", "queue_url", svc.queueURL, "message_id", *msg.MessageId, "retry_count", i, "max_retry_count", svc.maxDeleteRetry)
							time.Sleep(time.Duration(i*i) * time.Second)
							_, err = svc.sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
								ReceiptHandle: msg.ReceiptHandle,
								QueueUrl:      aws.String(svc.queueURL),
							})
							if err == nil {
								svc.logger.InfoContext(ctx, "message was deleted successfuly", "queue_url", svc.queueURL, "message_id", *msg.MessageId)
								break
							}
							svc.logger.WarnContext(ctx, "can not delete message from sqs queue", "queue_url", svc.queueURL, "message_id", *msg.MessageId, "error", err)
							if i == svc.maxDeleteRetry {
								svc.logger.WarnContext(ctx, "can not delete message from sqs queue but max retry count reached", "queue_url", svc.queueURL, "message_id", *msg.MessageId, "error", err, "receipt_handle", *msg.ReceiptHandle)
								errMu.Lock()
								errs = append(errs, err)
								errMu.Unlock()
							}
						}
					}
				}(msg)
			}
			wg.Wait()
			if len(errs) > 0 {
				return errors.Join(errs...)
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
}

func convertQueueURLToARN(urlObj *url.URL) (*arn.ARN, error) {
	if !strings.HasSuffix(strings.ToLower(urlObj.Host), ".amazonaws.com") || !strings.HasPrefix(strings.ToLower(urlObj.Host), "sqs.") {
		return nil, errors.New("invalid queue url")
	}
	part := strings.Split(strings.TrimLeft(urlObj.Path, "/"), "/")
	if len(part) != 2 {
		return nil, errors.New("invalid queue url")
	}
	awsRegion := strings.TrimSuffix(strings.TrimPrefix(strings.ToLower(urlObj.Host), "sqs."), ".amazonaws.com")
	arnObj := &arn.ARN{
		Partition: "aws",
		Service:   "sqs",
		Region:    awsRegion,
		AccountID: part[0],
		Resource:  part[1],
	}
	return arnObj, nil
}

func convertMessageToEventRecord(queueARN *arn.ARN, message *types.Message) events.SQSMessage {
	mesageAttributes := make(map[string]events.SQSMessageAttribute, len(message.MessageAttributes))
	for key, value := range message.MessageAttributes {
		mesageAttributes[key] = events.SQSMessageAttribute{
			StringValue:      value.StringValue,
			BinaryValue:      value.BinaryValue,
			StringListValues: value.StringListValues,
			BinaryListValues: value.BinaryListValues,
			DataType:         *value.DataType,
		}
	}
	return events.SQSMessage{
		MessageId:              coalesce(message.MessageId),
		ReceiptHandle:          coalesce(message.ReceiptHandle),
		Body:                   coalesce(message.Body),
		Md5OfBody:              coalesce(message.MD5OfBody),
		Md5OfMessageAttributes: coalesce(message.MD5OfMessageAttributes),
		Attributes:             message.Attributes,
		MessageAttributes:      mesageAttributes,
		EventSourceARN:         queueARN.String(),
		EventSource:            "aws:sqs",
		AWSRegion:              queueARN.Region,
	}
}

// fakeSQSClient は in-memoryで動作する。SQSClientの代用として使えるもの。動作としては透過的になっている。
// SendMessageで送られたものがmemoryに保持されて、ReceiveMessageで取り出せる。
// MessageIdはUUIDがランダムで生成される。
// ReceiptHandleはランダムなbytesをbase64でエンコードしたものが生成される。
// VisibirityTimeoutが考慮されており、一度ReceiveMessageで読まれたものは、VisibilityTimeoutの時間だけ、もう一度ReceivedMessageされても取り出せない。
// DeleteMessageは、VisibilityTimeoutの時間の間でのみ有効で、それ以外の時間ではエラーになる。
// また、VisibilityTimeoutはReceiveMessageの時に指定されたものが使われる。

type fakeSQSClient struct {
	once                     sync.Once
	mu                       sync.Mutex
	messages                 map[string]*types.Message
	isProcessing             map[string]bool
	approximateReceiveCount  map[string]int
	processingStartTime      map[string]time.Time
	messageIDByReceiptHandle map[string]string
	maxReceiveCount          int
	visibilityTimeout        time.Duration
	logger                   *slog.Logger
	dlq                      *json.Encoder
}

func (c *fakeSQSClient) prepare() {
	c.once.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.messages == nil {
			c.messages = make(map[string]*types.Message)
		}
		if c.isProcessing == nil {
			c.isProcessing = make(map[string]bool)
		}
		if c.approximateReceiveCount == nil {
			c.approximateReceiveCount = make(map[string]int)
		}
		if c.processingStartTime == nil {
			c.processingStartTime = make(map[string]time.Time)
		}
		if c.messageIDByReceiptHandle == nil {
			c.messageIDByReceiptHandle = make(map[string]string)
		}
		if c.visibilityTimeout == 0 {
			c.visibilityTimeout = 30 * time.Second
		}
		if c.maxReceiveCount == 0 {
			c.maxReceiveCount = 3
		}
		if c.logger == nil {
			c.logger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
		}
		if c.dlq == nil {
			c.dlq = json.NewEncoder(io.Discard)
		}
	})
}
func (c *fakeSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	c.prepare()
	msg := &types.Message{
		MessageId: aws.String(uuid.New().String()),
		Body:      params.MessageBody,
		MD5OfBody: aws.String(md5Digest(*params.MessageBody)),
		Attributes: map[string]string{
			"SenderId":      "AROAIWPX5BD2BHG722MW4:sender",
			"SentTimestamp": fmt.Sprintf("%d", time.Now().UnixMilli()),
		},
		MessageAttributes: make(map[string]types.MessageAttributeValue, len(params.MessageAttributes)),
	}
	var keys []string
	for k, v := range params.MessageAttributes {
		msg.MessageAttributes[k] = v
		keys = append(keys, k)
	}
	sort.StringSlice(keys).Sort()
	var builder strings.Builder
	for _, k := range keys {
		v := msg.MessageAttributes[k]
		builder.WriteString(k)
		builder.WriteString(*v.DataType)
		if v.StringValue != nil {
			builder.WriteString(*v.StringValue)
		}
		if len(v.StringListValues) > 0 {
			builder.WriteString(strings.Join(v.StringListValues, ""))
		}
		if v.BinaryValue != nil {
			builder.WriteString(base64.StdEncoding.EncodeToString(v.BinaryValue))
		}
		if len(v.BinaryListValues) > 0 {
			for _, b := range v.BinaryListValues {
				builder.WriteString(base64.StdEncoding.EncodeToString(b))
			}
		}
	}
	msg.MD5OfMessageAttributes = aws.String(md5Digest(builder.String()))
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages[*msg.MessageId] = msg
	c.logger.DebugContext(ctx, "enqueue to on memory queue", "current_queue_size", len(c.messages), "enqueued_message_id", *msg.MessageId)
	return &sqs.SendMessageOutput{
		MessageId: msg.MessageId,
	}, nil
}

func (c *fakeSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	c.prepare()
	output := &sqs.ReceiveMessageOutput{}
	outputInclude := make(map[string]bool, params.MaxNumberOfMessages)
	waitTimer := time.NewTimer(time.Duration(params.WaitTimeSeconds) * time.Second)
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, msg := range output.Messages {
			c.processingStartTime[*msg.MessageId] = time.Now()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return &sqs.ReceiveMessageOutput{}, ctx.Err()
		case <-waitTimer.C:
			return output, nil
		default:
		}
		if len(output.Messages) >= int(params.MaxNumberOfMessages) {
			break
		}
		time.Sleep(100 * time.Millisecond)
		func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			keys := make([]string, 0, len(c.messages))
			for k := range c.messages {
				keys = append(keys, k)
			}
			for _, key := range keys {
				if len(output.Messages) >= int(params.MaxNumberOfMessages) {
					return
				}
				if outputInclude[key] {
					continue
				}
				msg := c.messages[key]
				if is, ok := c.isProcessing[key]; ok && is {
					if time.Since(c.processingStartTime[key]) < c.visibilityTimeout {
						continue
					}
					delete(c.isProcessing, key)
					delete(c.processingStartTime, key)
					delete(c.messageIDByReceiptHandle, *msg.ReceiptHandle)
					if c.approximateReceiveCount[key] >= c.maxReceiveCount {
						c.logger.InfoContext(ctx, "delete message because approximate receive count reached to max recevice count", "message_id", key, "approximate_receive_count", c.approximateReceiveCount[*msg.MessageId], "max_receive_count", c.maxReceiveCount)
						c.dlq.Encode(msg)
						delete(c.messages, key)
						delete(c.approximateReceiveCount, key)
					}
					continue
				}
				c.isProcessing[key] = true
				c.processingStartTime[key] = time.Now()
				c.approximateReceiveCount[key]++
				receiptHandle := fmt.Sprintf(
					"%s/%s",
					base64.StdEncoding.EncodeToString(randomBytes(32)),
					base64.StdEncoding.EncodeToString(randomBytes(50)),
				)
				c.messageIDByReceiptHandle[receiptHandle] = key
				msg.ReceiptHandle = aws.String(receiptHandle)
				msg.Attributes["ApproximateReceiveCount"] = fmt.Sprintf("%d", c.approximateReceiveCount[key])
				if c.approximateReceiveCount[key] == 1 {
					msg.Attributes["ApproximateFirstReceiveTimestamp"] = fmt.Sprintf("%d", time.Now().UnixMilli())
				}
				c.messages[key] = msg
				outputInclude[key] = true
				output.Messages = append(output.Messages, *msg)
			}
		}()
	}
	return output, nil
}

func (c *fakeSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	c.prepare()
	c.mu.Lock()
	defer c.mu.Unlock()
	msgID, ok := c.messageIDByReceiptHandle[*params.ReceiptHandle]
	if !ok {
		return nil, errors.New("invalid receipt handle")
	}
	delete(c.messages, msgID)
	delete(c.isProcessing, msgID)
	delete(c.approximateReceiveCount, msgID)
	delete(c.processingStartTime, msgID)
	delete(c.messageIDByReceiptHandle, *params.ReceiptHandle)
	return &sqs.DeleteMessageOutput{}, nil
}

func (c *fakeSQSClient) GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	c.prepare()
	return &sqs.GetQueueUrlOutput{
		QueueUrl: aws.String(fmt.Sprintf("https://sqs.ap-northeast-1.amazonaws.com/123456789012/%s", *params.QueueName)),
	}, nil
}

func (c *fakeSQSClient) GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	c.prepare()
	return &sqs.GetQueueAttributesOutput{
		Attributes: map[string]string{
			"VisibilityTimeout": fmt.Sprintf("%d", int(c.visibilityTimeout.Seconds())),
		},
	}, nil
}

func (c *fakeSQSClient) MessageCount() int {
	c.prepare()
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.messages)
}
