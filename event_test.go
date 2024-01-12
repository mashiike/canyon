package canyon

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEventPayload__SQSEvent(t *testing.T) {
	var p eventPayload
	err := p.UnmarshalJSON(ReadFile(t, "testdata/sqs_event.json"))
	require.NoError(t, err, "should unmarshal")
	require.True(t, p.IsSQSEvent, "should be sqs event")
	require.Equal(t, 1, len(p.SQSEvent.Records), "should have 1 message")
	require.JSONEq(t, string(ReadFile(t, "testdata/serialized_http_request.json")), p.SQSEvent.Records[0].Body, "should have message body")
}

func TestEventPayload__HTTPEvent(t *testing.T) {
	var p eventPayload
	err := p.UnmarshalJSON(ReadFile(t, "testdata/http_event.json"))
	require.NoError(t, err, "should unmarshal")
	require.True(t, p.IsHTTPEvent, "should be http event")
	require.NotNil(t, p.Request, "should have http request")
	bs, err := io.ReadAll(p.Request.Body)
	require.NoError(t, err, "should read request body")
	require.EqualValues(t, "foo=bar%20baz", string(bs), "should have message body")
}

func TestEventPayload__WebsocketConnectEvent(t *testing.T) {
	var p eventPayload
	err := p.UnmarshalJSON(ReadFile(t, "testdata/ws_connect.json"))
	require.NoError(t, err, "should unmarshal")
	require.True(t, p.IsWebsocketProxyEvent, "should be websocket connect event")
	require.NotNil(t, p.Request, "should have websocket proxy request")
	require.NotNil(t, p.RequestContext, "should have websocket proxy request context")
	require.EqualValues(t, "$connect", WebsocketRouteKey(p.Request), "should have route key")
	require.EqualValues(t, "ZZZZZZZZZZZZZZZ=", WebsocketConnectionID(p.Request), "should have connection id")
}

func TestEventPayload__WebsocketDisconnectEvent(t *testing.T) {
	var p eventPayload
	err := p.UnmarshalJSON(ReadFile(t, "testdata/ws_disconnect.json"))
	require.NoError(t, err, "should unmarshal")
	require.True(t, p.IsWebsocketProxyEvent, "should be websocket disconnect event")
	require.NotNil(t, p.Request, "should have websocket proxy request")
	require.NotNil(t, p.RequestContext, "should have websocket proxy request context")
	require.EqualValues(t, "$disconnect", WebsocketRouteKey(p.Request), "should have route key")
	require.EqualValues(t, "ZZZZZZZZZZZZZZZ=", WebsocketConnectionID(p.Request), "should have connection id")
}

func TestEventPayload__WebsocketDefaultEvent(t *testing.T) {
	var p eventPayload
	err := p.UnmarshalJSON(ReadFile(t, "testdata/ws_default.json"))
	require.NoError(t, err, "should unmarshal")
	require.True(t, p.IsWebsocketProxyEvent, "should be websocket default event")
	require.NotNil(t, p.Request, "should have websocket proxy request")
	require.NotNil(t, p.RequestContext, "should have websocket proxy request context")
	require.EqualValues(t, "$default", WebsocketRouteKey(p.Request), "should have route key")
	require.EqualValues(t, "ZZZZZZZZZZZZZZZ=", WebsocketConnectionID(p.Request), "should have connection id")
}

func TestEventPayload__WebsocketMessageEvent(t *testing.T) {
	var p eventPayload
	err := p.UnmarshalJSON(ReadFile(t, "testdata/ws_message.json"))
	require.NoError(t, err, "should unmarshal")
	require.True(t, p.IsWebsocketProxyEvent, "should be websocket message event")
	require.NotNil(t, p.Request, "should have websocket proxy request")
	require.NotNil(t, p.RequestContext, "should have websocket proxy request context")
	require.EqualValues(t, "hello", WebsocketRouteKey(p.Request), "should have message")
	require.EqualValues(t, "ZZZZZZZZZZZZZZZ=", WebsocketConnectionID(p.Request), "should have connection id")
}

func TestEventPayload__AnyEvent(t *testing.T) {
	var p eventPayload
	err := p.UnmarshalJSON([]byte(`{"hoge": "fuga"}`))
	require.Error(t, err, "should failed unmarshal")
	require.EqualError(t, err, "can not unmarshal as sqs event: no Records\ncan not unmarshal as http event: no Host and Method\ncan not unmarshal as websocket proxy event: no APIID and ConnectionID")
}
