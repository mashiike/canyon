package canyon

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/fujiwara/ridge"
)

type eventPayload struct {
	IsSQSEvent bool
	SQSEvent   *events.SQSEvent

	IsHTTPEvent           bool
	IsWebsocketProxyEvent bool
	Request               *http.Request
}

func (p *eventPayload) UnmarshalJSON(bs []byte) error {
	var sqsEvent events.SQSEvent
	sqsUnmarshalErr := json.Unmarshal(bs, &sqsEvent)
	if sqsUnmarshalErr == nil {
		if len(sqsEvent.Records) == 0 {
			sqsUnmarshalErr = errors.New("no Records")
		} else {
			allCanyonEvent := true
			for _, record := range sqsEvent.Records {
				if record.EventSource != "aws:sqs" {
					allCanyonEvent = false
					break
				}
				if record.MessageAttributes == nil {
					allCanyonEvent = false
					break
				}
				sender, ok := record.MessageAttributes["Sender"]
				if !ok {
					allCanyonEvent = false
					break
				}
				if sender.DataType != "String" {
					allCanyonEvent = false
					break
				}
				if *sender.StringValue != "canyon" {
					allCanyonEvent = false
					break
				}
			}
			if allCanyonEvent {
				p.IsSQSEvent = true
				p.SQSEvent = &sqsEvent
				return nil
			}
			sqsUnmarshalErr = errors.New("not all records are from aws:sqs")
		}
	}
	req, newRequestErr := ridge.NewRequest(bs)
	if newRequestErr == nil {
		if req.Host != "" && req.Method != "" {
			p.IsHTTPEvent = true
			p.Request = req
			return nil
		}
		newRequestErr = errors.New("no Host and Method")
	}
	req, reqCtx, newWebsocketProxyRequestErr := newWebsocketProxyRequest(bs)
	if newWebsocketProxyRequestErr == nil {
		if reqCtx.APIID != "" && reqCtx.ConnectionID != "" {
			p.IsWebsocketProxyEvent = true
			p.Request = req
			return nil
		}
		newWebsocketProxyRequestErr = errors.New("no APIID and ConnectionID")
	}
	return errors.Join(
		fmt.Errorf("can not unmarshal as sqs event: %w", sqsUnmarshalErr),
		fmt.Errorf("can not unmarshal as http event: %w", newRequestErr),
		fmt.Errorf("can not unmarshal as websocket proxy event: %w", newWebsocketProxyRequestErr),
	)
}

func newWebsocketProxyRequest(bs []byte) (*http.Request, *events.APIGatewayWebsocketProxyRequestContext, error) {
	var proxyReq events.APIGatewayWebsocketProxyRequest
	if err := json.Unmarshal(bs, &proxyReq); err != nil {
		return nil, nil, err
	}
	header := make(http.Header)
	for k, v := range proxyReq.MultiValueHeaders {
		for _, vv := range v {
			header.Add(k, vv)
		}
	}
	for k, v := range proxyReq.Headers {
		header.Set(k, v)
	}
	header.Del("Host")
	var b io.Reader
	if proxyReq.IsBase64Encoded {
		raw := make([]byte, len(proxyReq.Body))
		n, err := base64.StdEncoding.Decode(raw, []byte(proxyReq.Body))
		if err != nil {
			return nil, nil, err
		}
		b = bytes.NewReader(raw[0:n])
	} else {
		b = strings.NewReader(proxyReq.Body)
	}
	req, err := http.NewRequest(http.MethodGet, "", b)
	if err != nil {
		return nil, nil, err
	}
	req.Header = header
	req.Host = proxyReq.RequestContext.DomainName
	req.RemoteAddr = proxyReq.RequestContext.Identity.SourceIP
	req = SetAPIGatewayWebsocketProxyHeader(req, &proxyReq.RequestContext)
	return req, &proxyReq.RequestContext, err
}
