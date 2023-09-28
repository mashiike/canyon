package canyon

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Songmu/flextime"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/google/uuid"
)

type Scheduler interface {
	RegisterSchedule(ctx context.Context, msg *sqs.SendMessageInput) error
}

type EventBridgeSchedulerClient interface {
	CreateSchedule(ctx context.Context, params *scheduler.CreateScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error)
}

type EventBridgeScheduler struct {
	mu         sync.Mutex
	iamRoleARN string
	groupName  *string
	namePrefix string
	client     EventBridgeSchedulerClient
}

func NewEventBridgeScheduler(ctx context.Context, namePrefix string) (*EventBridgeScheduler, error) {
	s := &EventBridgeScheduler{
		namePrefix: namePrefix,
	}
	_, _, err := s.get(ctx)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *EventBridgeScheduler) SetIAMRoleARN(iamRoleARN string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	arnObj, err := arn.Parse(iamRoleARN)
	if err != nil {
		return fmt.Errorf("invalid iam role arn: %w", err)
	}
	if arnObj.Service != "iam" || arnObj.Resource != "role" {
		return fmt.Errorf("invalid iam role arn: %s", iamRoleARN)
	}
	s.iamRoleARN = iamRoleARN
	return nil
}

func (s *EventBridgeScheduler) SetGroupName(groupName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groupName = &groupName
}

func (s *EventBridgeScheduler) SetAPIClient(client EventBridgeSchedulerClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client = client
}

func (s *EventBridgeScheduler) get(ctx context.Context) (string, EventBridgeSchedulerClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.iamRoleARN != "" && s.client != nil {
		return s.iamRoleARN, s.client, nil
	}
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("failed to load aws config: %w", err)
	}
	if s.client == nil {
		s.client = scheduler.NewFromConfig(awsCfg)
	}
	if s.iamRoleARN == "" {
		stsClient := sts.NewFromConfig(awsCfg)
		output, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
		if err != nil {
			return "", nil, fmt.Errorf("failed to get caller identity: %w", err)
		}
		// arn:aws:sts::<account_id>:assumed-role/<IAMRoleARN>/<session_name>
		arnObj, err := arn.Parse(*output.Arn)
		if err != nil {
			return "", nil, fmt.Errorf("invalid caller identity arn: %w", err)
		}
		resourceParts := strings.Split(arnObj.Resource, "/")
		if arnObj.Service != "sts" || len(resourceParts) != 3 || resourceParts[0] != "assumed-role" {
			return "", nil, fmt.Errorf("unexpected caller identity arn: %s", *output.Arn)
		}
		s.iamRoleARN = fmt.Sprintf(
			"arn:aws:iam::%s:role/%s",
			arnObj.AccountID,
			strings.Split(arnObj.Resource, "/")[1],
		)
	}
	return s.iamRoleARN, s.client, nil
}

func (s *EventBridgeScheduler) RegisterSchedule(ctx context.Context, msg *sqs.SendMessageInput) error {
	now := flextime.Now()
	iamRoleARN, client, err := s.get(ctx)
	if err != nil {
		return fmt.Errorf("failed to get iam role arn: %w", err)
	}
	scheduledAt := now.Add(time.Duration(msg.DelaySeconds) * time.Second)
	name := fmt.Sprintf("%s%s", s.namePrefix, uuid.New().String())
	if len(name) > 64 {
		name = name[:64]
	}
	sqsArn, err := sqsQueueURLToArn(*msg.QueueUrl)
	if err != nil {
		return fmt.Errorf("failed to parse sqs queue url: %w", err)
	}
	output, err := client.CreateSchedule(ctx, &scheduler.CreateScheduleInput{
		Name:      aws.String(name),
		GroupName: s.groupName,
		FlexibleTimeWindow: &types.FlexibleTimeWindow{
			Mode: types.FlexibleTimeWindowModeOff,
		},
		ScheduleExpression:         aws.String(fmt.Sprintf("at(%s)", scheduledAt.Format("2006-01-02T15:04:05"))),
		ScheduleExpressionTimezone: aws.String(scheduledAt.Location().String()),
		Description:                aws.String(fmt.Sprintf("canyon delayed sqs message, scheduled at %s", scheduledAt.Format("2006-01-02T15:04:05"))),
		ActionAfterCompletion:      types.ActionAfterCompletionDelete,
		State:                      types.ScheduleStateEnabled,
		Target: &types.Target{
			Arn:     aws.String(sqsArn),
			RoleArn: aws.String(iamRoleARN),
			Input:   msg.MessageBody,
			SqsParameters: &types.SqsParameters{
				MessageGroupId: msg.MessageGroupId,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create schedule: %w", err)
	}
	slog.InfoContext(ctx, "create schedule", "name", name, "scheduled_at", scheduledAt, "arn", *output.ScheduleArn)
	return nil
}

type InMemoryScheduler struct {
	clientFunc func() SQSClient
}

func NewInMemoryScheduler(clientFunc func() SQSClient) *InMemoryScheduler {
	return &InMemoryScheduler{
		clientFunc: clientFunc,
	}
}

func (s *InMemoryScheduler) RegisterSchedule(ctx context.Context, msg *sqs.SendMessageInput) error {
	now := flextime.Now()
	id := uuid.New().String()
	go func() {
		timer := time.NewTimer(time.Duration(msg.DelaySeconds) * time.Second)
		defer timer.Stop()
		<-timer.C
		output, err := s.clientFunc().SendMessage(ctx, msg)
		if err != nil {
			slog.ErrorContext(ctx, "failed to send message", "error", err, "scheduled_at", now.Add(time.Duration(msg.DelaySeconds)*time.Second), "schedule_id", id)
			return
		}
		slog.InfoContext(ctx, "send message", "message_id", *output.MessageId, "scheduled_at", now.Add(time.Duration(msg.DelaySeconds)*time.Second), "schedule_id", id)
	}()
	slog.InfoContext(ctx, "register schedule", "scheduled_at", now.Add(time.Duration(msg.DelaySeconds)*time.Second), "schedule_id", id)
	return nil
}
