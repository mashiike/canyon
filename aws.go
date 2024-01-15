package canyon

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Songmu/flextime"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi"
	sdklambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
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
	ChangeMessageVisibilityBatch(ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
}

type S3Client interface {
	manager.UploadAPIClient
	manager.DownloadAPIClient
	s3.HeadObjectAPIClient
}

type ManagementAPIClient interface {
	PostToConnection(context.Context, *apigatewaymanagementapi.PostToConnectionInput, ...func(*apigatewaymanagementapi.Options)) (*apigatewaymanagementapi.PostToConnectionOutput, error)
	DeleteConnection(context.Context, *apigatewaymanagementapi.DeleteConnectionInput, ...func(*apigatewaymanagementapi.Options)) (*apigatewaymanagementapi.DeleteConnectionOutput, error)
	GetConnection(context.Context, *apigatewaymanagementapi.GetConnectionInput, ...func(*apigatewaymanagementapi.Options)) (*apigatewaymanagementapi.GetConnectionOutput, error)
}

func NewManagementAPIClient(awsCfg aws.Config, endpointURL string) (ManagementAPIClient, error) {
	if endpointURL == "" {
		return nil, errors.New("endpoint url is required")
	}
	return apigatewaymanagementapi.NewFromConfig(awsCfg, func(o *apigatewaymanagementapi.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
	}), nil
}

var (
	defaultAWSConfig   *aws.Config
	defaultAWSConfigMu sync.Mutex
)

func SetDefaultAWSConfig(cfg *aws.Config) {
	defaultAWSConfigMu.Lock()
	defer defaultAWSConfigMu.Unlock()
	defaultAWSConfig = cfg
}

func getDefaultAWSConfig() (aws.Config, error) {
	defaultAWSConfigMu.Lock()
	defer defaultAWSConfigMu.Unlock()
	if defaultAWSConfig != nil {
		return *defaultAWSConfig, nil
	}
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return *aws.NewConfig(), err
	}
	defaultAWSConfig = &cfg
	return *defaultAWSConfig, nil
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

func getVisibilityTimeout(ctx context.Context, queueURL string, sqsClient SQSClient) (int64, error) {
	getAttributes, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{"VisibilityTimeout"},
	})
	if err != nil {
		return 0, fmt.Errorf("can not get queue attributes: %w", err)
	}
	visibilityTimeoutStr, ok := getAttributes.Attributes["VisibilityTimeout"]
	if !ok {
		return 30, nil
	}
	visibilityTimeout, err := strconv.ParseInt(visibilityTimeoutStr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid visibility timeout: %w", err)
	}
	return visibilityTimeout, nil
}

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
	urlObj, err := parseURL(svc.queueURL)
	if err != nil {
		return fmt.Errorf("invalid queue url: %w", err)
	}
	visibilityTimeout, err := getVisibilityTimeout(ctx, svc.queueURL, svc.sqsClient)
	if err != nil {
		return fmt.Errorf("can not get visibility timeout: %w", err)
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
		return nil, errors.New("invalid queue url: no sqs.*.amazon.com")
	}
	part := strings.Split(strings.TrimLeft(urlObj.Path, "/"), "/")
	if len(part) != 2 {
		return nil, errors.New("invalid queue url: missing resource name")
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

type inMemorySQSClient struct {
	once                     sync.Once
	mu                       sync.Mutex
	messages                 map[string]*types.Message
	isProcessing             map[string]bool
	approximateReceiveCount  map[string]int
	processingStartTime      map[string]time.Time
	receivableTime           map[string]time.Time
	messageIDByReceiptHandle map[string]string
	maxReceiveCount          int
	visibilityTimeout        time.Duration
	messageVisibilityTimeout map[string]time.Duration
	logger                   *slog.Logger
	dlq                      *json.Encoder
}

func (c *inMemorySQSClient) prepare() {
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
		if c.receivableTime == nil {
			c.receivableTime = make(map[string]time.Time)
		}
		if c.visibilityTimeout == 0 {
			c.visibilityTimeout = 30 * time.Second
		}
		if c.messageVisibilityTimeout == nil {
			c.messageVisibilityTimeout = make(map[string]time.Duration)
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

func (c *inMemorySQSClient) ChangeMessageVisibilityBatch(ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	c.prepare()
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := &sqs.ChangeMessageVisibilityBatchOutput{
		Failed:     make([]types.BatchResultErrorEntry, 0, len(params.Entries)),
		Successful: make([]types.ChangeMessageVisibilityBatchResultEntry, 0),
	}
	for _, entry := range params.Entries {
		msgID, ok := c.messageIDByReceiptHandle[*entry.ReceiptHandle]
		if !ok {
			ret.Failed = append(ret.Failed, types.BatchResultErrorEntry{
				Id:          entry.Id,
				Code:        aws.String("ReceiptHandleIsInvalid"),
				Message:     aws.String("The input receipt handle is invalid."),
				SenderFault: true,
			})
			continue
		}
		if _, ok := c.messages[msgID]; !ok {
			ret.Failed = append(ret.Failed, types.BatchResultErrorEntry{
				Id:          entry.Id,
				Code:        aws.String("MessageNotExist"),
				Message:     aws.String("The message identified by the receipt handle does not exist or is not available for visibility timeout change."),
				SenderFault: true,
			})
		}
		c.messageVisibilityTimeout[msgID] = time.Since(c.processingStartTime[msgID]) + time.Duration(entry.VisibilityTimeout)*time.Second
		ret.Successful = append(ret.Successful, types.ChangeMessageVisibilityBatchResultEntry{
			Id: entry.Id,
		})
	}
	return ret, nil
}

func (c *inMemorySQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
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
	receivableTime := flextime.Now()
	if params.DelaySeconds > 0 {
		receivableTime = receivableTime.Add(time.Duration(params.DelaySeconds) * time.Second)
	}
	c.receivableTime[*msg.MessageId] = receivableTime
	c.logger.DebugContext(ctx, "enqueue to on memory queue", "current_queue_size", len(c.messages), "enqueued_message_id", *msg.MessageId)
	return &sqs.SendMessageOutput{
		MessageId: msg.MessageId,
	}, nil
}

func (c *inMemorySQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
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
					if time.Since(c.processingStartTime[key]) < c.messageVisibilityTimeout[key] {
						continue
					}
					delete(c.isProcessing, key)
					delete(c.processingStartTime, key)
					delete(c.messageVisibilityTimeout, key)
					delete(c.messageIDByReceiptHandle, *msg.ReceiptHandle)
					continue
				}
				if receivableTime, ok := c.receivableTime[key]; ok && receivableTime.After(time.Now()) {
					continue
				}
				c.isProcessing[key] = true
				c.processingStartTime[key] = time.Now()
				if params.VisibilityTimeout > 0 {
					c.messageVisibilityTimeout[key] = time.Duration(params.VisibilityTimeout) * time.Second
				} else {
					c.messageVisibilityTimeout[key] = c.visibilityTimeout
				}
				c.approximateReceiveCount[key]++
				c.logger.DebugContext(ctx, "dequeue from on memory queue",
					"current_queue_size", len(c.messages),
					"dequeued_message_id", key,
					"approximate_receive_count", c.approximateReceiveCount[key],
				)
				if c.approximateReceiveCount[key] > c.maxReceiveCount {
					c.logger.InfoContext(ctx, "delete message because approximate receive count reached to max recevice count", "message_id", key, "approximate_receive_count", c.approximateReceiveCount[*msg.MessageId], "max_receive_count", c.maxReceiveCount)
					c.dlq.Encode(msg)
					delete(c.messages, key)
					delete(c.approximateReceiveCount, key)
					delete(c.isProcessing, key)
					delete(c.processingStartTime, key)
					delete(c.messageVisibilityTimeout, key)
					delete(c.messageIDByReceiptHandle, *msg.ReceiptHandle)
					continue
				}
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

func (c *inMemorySQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
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

func (c *inMemorySQSClient) GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	c.prepare()
	return &sqs.GetQueueUrlOutput{
		QueueUrl: aws.String(fmt.Sprintf("https://sqs.ap-northeast-1.amazonaws.com/123456789012/%s", *params.QueueName)),
	}, nil
}

func (c *inMemorySQSClient) GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	c.prepare()
	return &sqs.GetQueueAttributesOutput{
		Attributes: map[string]string{
			"VisibilityTimeout": fmt.Sprintf("%d", int(c.visibilityTimeout.Seconds())),
		},
	}, nil
}

func (c *inMemorySQSClient) MessageCount() int {
	c.prepare()
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.messages)
}

// S3Backend is a backend for saving and loading request body to/from S3.
type S3Backend struct {
	s3URLPrefix  *url.URL
	once         sync.Once
	initErr      error
	s3Client     S3Client
	uploader     *manager.Uploader
	downloader   *manager.Downloader
	uploaderName string
}

// NewS3Backend creates a new S3Backend.
func NewS3Backend(s3URLPrefix string) (*S3Backend, error) {
	s3URL, err := parseURL(s3URLPrefix)
	if err != nil {
		return nil, fmt.Errorf("parse failed url: %w", err)
	}
	if !isS3URL(s3URL) {
		return nil, fmt.Errorf("invalid s3 url: %s", s3URL.String())
	}
	return &S3Backend{
		s3URLPrefix: s3URL,
	}, err
}

func (b *S3Backend) init() {
	b.once.Do(func() {
		if b.s3Client == nil {
			awsCfg, err := getDefaultAWSConfig()
			if err != nil {
				b.initErr = err
			}
			b.SetS3Client(s3.NewFromConfig(awsCfg))
		}
	})
}

// SetS3Client sets S3Client to S3Backend.
// for testing.
func (b *S3Backend) SetS3Client(s3Client S3Client) {
	b.initErr = nil
	b.s3Client = s3Client
	b.uploader = manager.NewUploader(s3Client)
	b.downloader = manager.NewDownloader(s3Client)
}

// SetAppName sets uploader name to S3Backend.
//
//	this value is used for metadata of S3 object.
func (b *S3Backend) SetAppName(name string) {
	b.uploaderName = name
}

func (b *S3Backend) SaveRequestBody(ctx context.Context, req *http.Request) (*url.URL, error) {
	b.init()
	if b.initErr != nil {
		return nil, fmt.Errorf("failed to initialize s3 backend: %w", b.initErr)
	}
	now := time.Now()
	u := b.s3URLPrefix.JoinPath(
		now.Format("2006/01/02/15/"),
		fmt.Sprintf(
			"%s-%s%s",
			now.Format("20060102-150405"),
			uuid.New().String(),
			getExtension(req.Header.Get("Content-Type")),
		),
	)
	objectKey := strings.TrimLeft(u.Path, "/")
	params := &s3.PutObjectInput{
		Bucket:      aws.String(u.Host),
		Key:         aws.String(objectKey),
		Body:        req.Body,
		ContentType: aws.String(req.Header.Get("Content-Type")),
		Metadata: map[string]string{
			"uploader":                         "canyon",
			"request-url":                      req.URL.String(),
			"request-method":                   req.Method,
			"request-remote-addr":              req.RemoteAddr,
			"request-host":                     req.Host,
			"request-user-agent":               req.UserAgent(),
			"request-traceId":                  req.Header.Get("X-Amzn-Trace-Id"),
			"request-header-x-forwarded-for":   req.Header.Get("X-Forwarded-For"),
			"request-header-x-forwarded-proto": req.Header.Get("X-Forwarded-Proto"),
			"request-header-x-forwarded-port":  req.Header.Get("X-Forwarded-Port"),
		},
	}
	if b.uploaderName != "" {
		params.Metadata["uploader"] = b.uploaderName
	}
	lc, ok := lambdacontext.FromContext(ctx)
	if ok {
		params.Metadata["invoked-lambda-fuction-arn"] = lc.InvokedFunctionArn
		params.Metadata["lambda-fuction-name"] = lambdacontext.FunctionName
		params.Metadata["lambda-fuction-version"] = lambdacontext.FunctionVersion
	}
	defer req.Body.Close()
	_, err := b.uploader.Upload(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to upload request: %w", err)
	}
	return u, nil
}

func (b *S3Backend) LoadRequestBody(ctx context.Context, u *url.URL) (io.ReadCloser, error) {
	b.init()
	if b.initErr != nil {
		return nil, fmt.Errorf("failed to initialize s3 backend: %w", b.initErr)
	}
	if !isS3URL(u) {
		return nil, fmt.Errorf("invalid s3 url: %s", u.String())
	}
	objectKey := strings.TrimLeft(u.Path, "/")
	head, err := b.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(u.Host),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to head object: %w", err)
	}
	buf := manager.NewWriteAtBuffer(make([]byte, *head.ContentLength))
	_, err = b.downloader.Download(context.Background(), buf, &s3.GetObjectInput{
		Bucket: aws.String(u.Host),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download request: %w", err)
	}
	objectBody := buf.Bytes()[:*head.ContentLength]
	return io.NopCloser(bytes.NewReader(objectBody)), nil
}

var (
	enableReportBatchItemFailures  map[string]bool = make(map[string]bool)
	onceCheckFunctionResponseTypes sync.Once
)

func checkFunctionResponseTypes(ctx context.Context, c *runOptions) {
	defer func() {
		for k, v := range enableReportBatchItemFailures {
			if v {
				c.InfoContextWhenVarbose(ctx, "enable report batch item failures", "event_source_arn", k)
			}
		}
	}()

	awsCfg, err := getDefaultAWSConfig()
	if err != nil {
		return
	}
	lambdaClient := sdklambda.NewFromConfig(awsCfg)
	lc, ok := lambdacontext.FromContext(ctx)
	if !ok {
		c.WarnContextWhenVarbose(ctx, "missing lambda context for check function response types")
		return
	}
	p := sdklambda.NewListEventSourceMappingsPaginator(lambdaClient, &sdklambda.ListEventSourceMappingsInput{
		FunctionName: &lc.InvokedFunctionArn,
	})
	for p.HasMorePages() {
		list, err := p.NextPage(ctx)
		if err != nil {
			c.WarnContextWhenVarbose(ctx, "failed to list event source mappings", "error", err)
			return
		}
		for _, m := range list.EventSourceMappings {
			if m.EventSourceArn == nil {
				continue
			}
			if !strings.HasPrefix(*m.EventSourceArn, "arn:aws:sqs:") {
				continue
			}
			for _, v := range m.FunctionResponseTypes {
				if v == lambdatypes.FunctionResponseTypeReportBatchItemFailures {
					enableReportBatchItemFailures[*m.EventSourceArn] = true
				}
			}
		}
	}
}

func isEnableReportBatchItemFailures(ctx context.Context, eventSourceARN string) bool {
	return enableReportBatchItemFailures[eventSourceARN]
}
