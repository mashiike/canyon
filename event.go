package canyon

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/fujiwara/ridge"
)

type eventPayload struct {
	IsSQSEvent bool
	SQSEvent   *events.SQSEvent

	IsHTTPEvent bool
	Request     *http.Request
}

func (p *eventPayload) UnmarshalJSON(bs []byte) error {
	var sqsEvent events.SQSEvent
	sqsUnmarshalErr := json.Unmarshal(bs, &sqsEvent)
	if sqsUnmarshalErr == nil {
		if len(sqsEvent.Records) > 0 {
			p.IsSQSEvent = true
			p.SQSEvent = &sqsEvent
			return nil
		}
		sqsUnmarshalErr = errors.New("no Records")
	}
	req, newRequestErr := ridge.NewRequest(bs)
	if newRequestErr == nil {
		if req.Host != "" {
			p.IsHTTPEvent = true
			p.Request = req
			return nil
		}
		newRequestErr = errors.New("no Host")
	}
	return errors.Join(
		fmt.Errorf("can not unmarshal as sqs event: %w", sqsUnmarshalErr),
		fmt.Errorf("can not unmarshal as http event: %w", newRequestErr),
	)
}
