package canyontest

import (
	"context"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/mashiike/canyon"
)

// DummySQSMessage is a dummy sqs message for testing.
var DummySQSMessage = events.SQSMessage{
	MessageId:     "00000000-0000-0000-0000-000000000000",
	ReceiptHandle: "00000000/0000000000=",
	Body:          "{}",
	Attributes: map[string]string{
		"ApproximateReceiveCount":          "1",
		"SentTimestamp":                    "0",
		"SenderId":                         "000000000000",
		"ApproximateFirstReceiveTimestamp": "0",
	},
	MessageAttributes: map[string]events.SQSMessageAttribute{
		"Value": {
			StringValue: aws.String("1"),
			DataType:    "Number",
		},
	},
	EventSourceARN: "arn:aws:sqs:ap-northeast-1:123456789012:canyon-test",
	EventSource:    "aws:sqs",
	AWSRegion:      "ap-northeast-1",
}

// AsServer returns http.Handler that embeds dummy sqs message hader and worker flag in context.
func AsWorker(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := canyon.EmbedIsWorkerInContext(r.Context(), true)
		r = canyon.SetSQSMessageHeader(r, &DummySQSMessage)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// AsServer returns http.Handler that embeds logger and sqs message sender in context.
func AsServer(next http.Handler, sender canyon.WorkerSender) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sender == nil {
			sender = canyon.WorkerSenderFunc(func(r *http.Request, opts *canyon.SendOptions) (string, error) {
				return DummySQSMessage.MessageId, nil
			})
		}
		ctx := canyon.EmbedWorkerSenderInContext(r.Context(), sender)
		ctx = canyon.EmbedIsWorkerInContext(ctx, false)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// AsWebsocket returns http.Handler that embeds logger and sqs message sender in context.
func AsWebsocket(next http.Handler, reqCtx events.APIGatewayWebsocketProxyRequestContext, sender canyon.WorkerSender) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := canyon.EmbedIsWorkerInContext(r.Context(), false)
		ctx = canyon.EmbedWorkerSenderInContext(ctx, sender)
		r = canyon.SetAPIGatewayWebsocketProxyHeader(r, &reqCtx)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// AsLambda returns lambda.Handler that embeds logger and sqs message sender in context.
func AsLambdaFallback(next lambda.Handler, sender canyon.WorkerSender) lambda.Handler {
	return canyon.LambdaHandlerFunc(
		func(ctx context.Context, event []byte) ([]byte, error) {
			if sender == nil {
				sender = canyon.WorkerSenderFunc(func(r *http.Request, opts *canyon.SendOptions) (string, error) {
					return DummySQSMessage.MessageId, nil
				})
			}
			ctx = canyon.EmbedWorkerSenderInContext(ctx, sender)
			ctx = canyon.EmbedIsWorkerInContext(ctx, false)
			return next.Invoke(ctx, event)
		},
	)
}
