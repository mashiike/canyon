package canyon

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Scheduler interface {
	RegisterSchedule(ctx context.Context, msg *sqs.SendMessageInput) error
}
