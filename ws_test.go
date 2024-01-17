package canyon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

type mutexBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (mb *mutexBuffer) Write(p []byte) (n int, err error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.buf.Write(p)
}

func (mw *mutexBuffer) String() string {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	return mw.buf.String()
}

func TestWebsocketHTTPBridgeHandler(t *testing.T) {
	var mu sync.Mutex
	var mgrAPIClient ManagementAPIClient
	connectionIDs := make(map[string]struct{})
	handler := NewWebsocketHTTPBridgeHandler(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := EmbedWebsocketManagementAPIClient(req.Context(), mgrAPIClient)
			connectionID := WebsocketConnectionID(req)
			switch WebsocketRouteKey(req) {
			case "$connect":
				mu.Lock()
				connectionIDs[connectionID] = struct{}{}
				mu.Unlock()
				w.WriteHeader(http.StatusOK)
			case "$disconnect":
				mu.Lock()
				delete(connectionIDs, connectionID)
				mu.Unlock()
			case "$default":
				bs, err := io.ReadAll(req.Body)
				if err != nil {
					slog.Error("read body failed", "detail", err)
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprint(w, http.StatusText(http.StatusInternalServerError))
					return
				}
				if bytes.EqualFold(bs, []byte("exit")) {
					if err := DeleteConnection(ctx, connectionID); err != nil {
						slog.Error("delete connection failed", "detail", err)
						w.WriteHeader(http.StatusInternalServerError)
						fmt.Fprint(w, http.StatusText(http.StatusInternalServerError))
					}
					w.WriteHeader(http.StatusOK)
					fmt.Fprint(w, http.StatusText(http.StatusOK))
					return
				}
				if bytes.Equal(bs, []byte("me")) {
					w.WriteHeader(http.StatusOK)
					fmt.Fprint(w, connectionID)
					return
				}
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, http.StatusText(http.StatusOK))
			case "echo":
				bs, err := io.ReadAll(req.Body)
				if err != nil {
					slog.Error("read body failed", "detail", err)
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprint(w, http.StatusText(http.StatusInternalServerError))
					return
				}
				w.WriteHeader(http.StatusOK)
				w.Write(bs)
			case "whisper":
				var data map[string]interface{}
				err := json.NewDecoder(req.Body).Decode(&data)
				if err != nil {
					slog.Error("unmarshal failed", "detail", err)
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, http.StatusText(http.StatusBadRequest))
					return
				}
				connectionID, ok := data["connectionID"].(string)
				if !ok {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, http.StatusText(http.StatusBadRequest))
					return
				}
				msg, ok := data["message"].(string)
				if !ok {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, http.StatusText(http.StatusBadRequest))
					return
				}
				err = PostToConnection(ctx, connectionID, []byte(msg))
				if err != nil {
					slog.Error("post to failed", "detail", err, "connectionID", connectionID)
					if !ConnectionIsGone(err) {
						w.WriteHeader(http.StatusInternalServerError)
						fmt.Fprint(w, http.StatusText(http.StatusInternalServerError))
					} else {
						w.WriteHeader(http.StatusGone)
						fmt.Fprint(w, http.StatusText(http.StatusGone))
					}
					return
				}
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, http.StatusText(http.StatusOK))
			case "lookup":
				var data map[string]interface{}
				err := json.NewDecoder(req.Body).Decode(&data)
				if err != nil {
					slog.Error("unmarshal failed", "detail", err)
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, http.StatusText(http.StatusBadRequest))
					return
				}
				connectionID, ok := data["connectionID"].(string)
				if !ok {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, http.StatusText(http.StatusBadRequest))
					return
				}
				info, err := GetConnection(ctx, connectionID)
				if err != nil {
					slog.Error("get connection failed", "detail", err, "connectionID", connectionID)
					if !ConnectionIsGone(err) {
						w.WriteHeader(http.StatusInternalServerError)
						fmt.Fprint(w, http.StatusText(http.StatusInternalServerError))
					} else {
						w.WriteHeader(http.StatusGone)
						fmt.Fprint(w, http.StatusText(http.StatusGone))
					}
					return
				}
				bs, err := json.Marshal(info)
				if err != nil {
					slog.Error("marshal failed", "detail", err, "connectionID", connectionID)
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprint(w, http.StatusText(http.StatusInternalServerError))
					return
				}
				w.WriteHeader(http.StatusOK)
				w.Write(bs)
			default:
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, http.StatusText(http.StatusNotFound))
			}
		}),
	)
	var buf mutexBuffer
	logger := slog.New(slog.NewJSONHandler(
		&buf,
		&slog.HandlerOptions{
			Level: slog.LevelDebug,
		},
	))
	t.Cleanup(func() {
		t.Log(buf.String())
	})
	handler.SetLogger(logger)
	handler.SetVerbose(true)
	server := httptest.NewServer(handler)
	defer server.Close()
	var err error
	mgrAPIClient, err = NewManagementAPIClient(
		aws.Config{
			Region: "ap-northeast-1",
			Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
				"AWS_ACCESS_KEY_ID",
				"AWS_SECRET_ACCESS_KEY",
				"AWS_SESSION_TOKEN",
			)),
		}, server.URL)
	require.NoError(t, err)
	connectURL := "ws://" + server.Listener.Addr().String() + "/"

	t.Run("echo route", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, _, err := websocket.DefaultDialer.DialContext(ctx, connectURL, nil)
		require.NoError(t, err, "should dial")
		defer func() {
			msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Normal Closure")
			c.WriteMessage(websocket.CloseMessage, msg)
			c.Close()
		}()
		err = c.WriteMessage(websocket.TextMessage, []byte(`{"action":"echo", "hoge":"fuga"}`))
		require.NoError(t, err, "should write")
		_, message, err := c.ReadMessage()
		require.NoError(t, err, "should read")
		require.JSONEq(t, `{"action":"echo", "hoge":"fuga"}`, string(message), "should echo")
	})

	t.Run("send exit", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, _, err := websocket.DefaultDialer.DialContext(ctx, connectURL, nil)
		require.NoError(t, err, "should dial")
		defer func() {
			msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Normal Closure")
			c.WriteMessage(websocket.CloseMessage, msg)
			c.Close()
		}()
		err = c.WriteMessage(websocket.TextMessage, []byte(`exit`))
		require.NoError(t, err, "should write")
		_, _, err = c.ReadMessage()
		require.Error(t, err, "should close")
		var closeErr *websocket.CloseError
		require.ErrorAs(t, err, &closeErr, "should close")
		require.Equal(t, websocket.CloseNormalClosure, closeErr.Code, "should close normally")
	})

	t.Run("whisper", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c1, _, err := websocket.DefaultDialer.DialContext(ctx, connectURL, nil)
		if err != nil {
			log.Fatal("dial:", err)
		}
		c2, _, err := websocket.DefaultDialer.DialContext(ctx, connectURL, nil)
		if err != nil {
			log.Fatal("dial:", err)
		}
		defer func() {
			msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Normal Closure")
			c1.WriteMessage(websocket.CloseMessage, msg)
			c2.WriteMessage(websocket.CloseMessage, msg)
			c1.Close()
			c2.Close()
		}()
		if err := c1.WriteMessage(websocket.TextMessage, []byte(`me`)); err != nil {
			log.Fatal("write:", err)
		}
		_, connectionIDOfC1, err := c1.ReadMessage()
		if err != nil {
			log.Fatal("read:", err)
		}
		if _, ok := connectionIDs[string(connectionIDOfC1)]; !ok {
			t.Errorf("unexpected connectionID: `%s`", connectionIDOfC1)
		}
		if err := c2.WriteMessage(websocket.TextMessage, []byte(`{"action":"whisper", "connectionID":"`+string(connectionIDOfC1)+`", "message":"hello"}`)); err != nil {
			log.Fatal("write:", err)
		}
		_, resp, err := c2.ReadMessage()
		if err != nil {
			log.Fatal("read:", err)
		}
		if string(resp) != http.StatusText(http.StatusOK) {
			t.Errorf("unexpected response: `%s`", resp)
		}
		_, message, err := c1.ReadMessage()
		if err != nil {
			log.Fatal("read:", err)
		}
		if string(message) != "hello" {
			t.Errorf("unexpected message: `%s`", message)
		}
	})

	t.Run("lookup", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c1, _, err := websocket.DefaultDialer.DialContext(ctx, connectURL, nil)
		if err != nil {
			log.Fatal("dial:", err)
		}
		c2, _, err := websocket.DefaultDialer.DialContext(ctx, connectURL, nil)
		if err != nil {
			log.Fatal("dial:", err)
		}
		defer func() {
			msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Normal Closure")
			c1.WriteMessage(websocket.CloseMessage, msg)
			c2.WriteMessage(websocket.CloseMessage, msg)
			c1.Close()
			c2.Close()
		}()
		if err := c1.WriteMessage(websocket.TextMessage, []byte(`me`)); err != nil {
			log.Fatal("write:", err)
		}
		_, connectionIDOfC1, err := c1.ReadMessage()
		if err != nil {
			log.Fatal("read:", err)
		}
		if _, ok := connectionIDs[string(connectionIDOfC1)]; !ok {
			t.Errorf("unexpected connectionID: `%s`", connectionIDOfC1)
		}
		if err := c2.WriteMessage(websocket.TextMessage, []byte(`{"action":"lookup", "connectionID":"`+string(connectionIDOfC1)+`"}`)); err != nil {
			log.Fatal("write:", err)
		}
		_, resp, err := c2.ReadMessage()
		if err != nil {
			log.Fatal("read:", err)
		}
		var info struct {
			Identity struct {
				SourceIP  string `json:"sourceIp"`
				UserAgent string `json:"userAgent"`
			} `json:"identity"`
		}
		if err := json.Unmarshal(resp, &info); err != nil {
			t.Errorf("unexpected response: `%s`", resp)
		}
		if !strings.HasPrefix(info.Identity.SourceIP, "127.0.0.1:") {
			t.Errorf("unexpected response: `%s`", info.Identity.SourceIP)
		}
		if !strings.EqualFold(info.Identity.UserAgent, "Go-http-client/1.1") {
			t.Errorf("unexpected response: `%s`", info.Identity.UserAgent)
		}
	})
}
