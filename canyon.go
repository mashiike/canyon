package canyon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/fujiwara/ridge"
	"github.com/mashiike/canyon/internal/jsonx"
	"github.com/pires/go-proxyproto"
)

func Run(sqsQueueName string, mux http.Handler, opts ...Option) error {
	return RunWithContext(context.Background(), sqsQueueName, mux, opts...)
}

func RunWithContext(ctx context.Context, sqsQueueName string, mux http.Handler, opts ...Option) error {
	ctx, cancel := context.WithCancelCause(ctx)
	c := defaultRunConfig(cancel, sqsQueueName)
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	defer func() {
		c.Cleanup()
	}()
	if s, ok := c.serializer.(BackendSerializer); ok {
		slog.DebugContext(ctx, "use backend", "backend", c.backend)
		c.serializer = s.WithBackend(c.backend)
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	if c.disableWorker && c.disableServer {
		return errors.New("both worker and server are disabled")
	}
	if err := runWithContext(ctx, mux, c); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			cancel(err)
			return err
		}
	}
	if err := context.Cause(ctx); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
	}
	return nil
}

func runWithContext(ctx context.Context, mux http.Handler, c *runOptions) error {
	workerHandler := newWorkerHandler(mux, c)
	serverHandler := newServerHandler(mux, c)
	lambdaFallbackHandler := newLambdaFallbackHandler(mux, c)

	if OnLambdaRuntime() {
		return setupLambdaHandler(ctx, workerHandler, serverHandler, lambdaFallbackHandler, c)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if !c.disableServer {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := setupHTTPServer(cctx, serverHandler, c, cancel); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}

	if !c.disableWorker {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := setupSQSPoller(cctx, workerHandler, c, cancel); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}

	if !c.disableWebsocket && (c.websocketListener != nil || c.websocketAddress != "") {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := setupWebSocketServer(cctx, serverHandler, c, cancel); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}

	if lambdaFallbackHandler != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := setupFallbackLambdaHandler(cctx, lambdaFallbackHandler, c, cancel); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return context.Cause(ctx)
}

func setupLambdaHandler(_ context.Context, workerHandler sqsEventLambdaHandlerFunc, serverHandler http.Handler, lambdaFallbackHandler lambda.Handler, c *runOptions) error {
	handler := &lambdaHandler{
		workerHandler:         workerHandler,
		serverHandler:         serverHandler,
		lambdaFallbackHandler: lambdaFallbackHandler,
		runOptions:            c,
	}

	lambdaOptions := make([]lambda.Option, len(c.lambdaOptions))
	copy(lambdaOptions, c.lambdaOptions)

	lambdaHandler := lambda.NewHandlerWithOptions(handler.handleEvent, lambdaOptions...)
	for _, middleware := range c.lambdaMiddlewares {
		lambdaHandler = middleware(lambdaHandler)
	}
	lambda.Start(lambdaHandler)
	return nil
}

type lambdaHandler struct {
	workerHandler         sqsEventLambdaHandlerFunc
	serverHandler         http.Handler
	lambdaFallbackHandler lambda.Handler
	runOptions            *runOptions
}

func (h *lambdaHandler) handleEvent(ctx context.Context, event json.RawMessage) (interface{}, error) {
	var p eventPayload
	if err := json.Unmarshal(event, &p); err != nil {
		return h.handleFallback(ctx, event, err)
	}
	select {
	case <-ctx.Done():
		slog.DebugContext(ctx, "context done", "error", ctx.Err())
		return nil, ctx.Err()
	default:
	}
	switch {
	case p.IsSQSEvent:
		return h.handleSQSEvent(ctx, p.SQSEvent)
	case p.IsHTTPEvent:
		return h.handleHTTPEvent(ctx, p.Request)
	case p.IsWebsocketProxyEvent:
		return h.handleWebsocketEvent(ctx, p.Request)
	default:
		return h.handleFallback(ctx, event, errors.New("unsupported event"))
	}
}

func (h *lambdaHandler) handleSQSEvent(ctx context.Context, sqsEvent *events.SQSEvent) (interface{}, error) {
	if len(sqsEvent.Records) > 1 {
		onceCheckFunctionResponseTypes.Do(func() {
			checkFunctionResponseTypes(ctx, h.runOptions)
		})
	}

	resp, err := h.workerHandler(ctx, sqsEvent)
	if err != nil {
		return nil, err
	}
	if len(resp.BatchItemFailures) == 0 {
		return resp, nil
	}
	if len(sqsEvent.Records) == 1 {
		return resp, fmt.Errorf("failed processing record(message_id=%s)", sqsEvent.Records[0].MessageId)
	}
	if !isEnableReportBatchItemFailures(ctx, sqsEvent.Records[0].EventSourceARN) {
		return resp, fmt.Errorf("failed processing %d records", len(resp.BatchItemFailures))
	}
	return resp, nil
}

type httpStramingResponseWriter struct {
	header          http.Header
	pipeWriter      *io.PipeWriter
	buffer          bytes.Buffer
	isWrittenHeader bool
	ready           chan struct{}
	resp            events.LambdaFunctionURLStreamingResponse
}

func newHTTPStramingResponseWriter() *httpStramingResponseWriter {
	pipeReader, pipeWriter := io.Pipe()
	resp := events.LambdaFunctionURLStreamingResponse{
		Body: pipeReader,
	}
	return &httpStramingResponseWriter{
		header:     http.Header{},
		pipeWriter: pipeWriter,
		resp:       resp,
		ready:      make(chan struct{}),
	}
}

func (w *httpStramingResponseWriter) Header() http.Header {
	return w.header
}

func (w *httpStramingResponseWriter) WriteHeader(code int) {
	if w.isWrittenHeader {
		return
	}
	w.isWrittenHeader = true
	w.resp.StatusCode = code
	if len(w.header) > 0 {
		w.resp.Headers = make(map[string]string, len(w.header))
		for k, v := range w.header {
			if k == "Set-Cookie" {
				w.resp.Cookies = v
			} else {
				w.resp.Headers[k] = strings.Join(v, ",")
			}
		}
	}
	close(w.ready)
}

func (w *httpStramingResponseWriter) Write(b []byte) (int, error) {
	return w.buffer.Write(b)
}

func (w *httpStramingResponseWriter) Flush() {
	w.pipeWriter.Write(w.buffer.Bytes())
	w.buffer.Reset()
}

func (w *httpStramingResponseWriter) Response() *events.LambdaFunctionURLStreamingResponse {
	return &w.resp
}

func (h *lambdaHandler) handleHTTPEvent(ctx context.Context, req *http.Request) (interface{}, error) {
	r := req.WithContext(ctx)
	if !h.runOptions.invokeModeStramingResponse {
		w := ridge.NewResponseWriter()
		h.serverHandler.ServeHTTP(w, r)
		return w.Response(), nil
	}
	w := newHTTPStramingResponseWriter()
	go func() {
		defer func() {
			w.WriteHeader(http.StatusOK)
			w.Flush()
			w.pipeWriter.Close()
		}()
		h.serverHandler.ServeHTTP(w, r)
	}()
	<-w.ready
	return w.Response(), nil
}

func (h *lambdaHandler) handleWebsocketEvent(ctx context.Context, req *http.Request) (interface{}, error) {
	r := req.WithContext(ctx)
	w := ridge.NewResponseWriter()
	h.serverHandler.ServeHTTP(w, r)
	return w.Response(), nil
}

func (h *lambdaHandler) handleFallback(ctx context.Context, event json.RawMessage, err error) (interface{}, error) {
	if h.lambdaFallbackHandler != nil {
		return h.lambdaFallbackHandler.Invoke(ctx, event)
	}
	return nil, err
}

func setupHTTPServer(ctx context.Context, serverHandler http.Handler, c *runOptions, cancel context.CancelFunc) error {
	m := http.NewServeMux()
	switch {
	case c.prefix == "/", c.prefix == "":
		m.Handle("/", serverHandler)
	case !strings.HasSuffix(c.prefix, "/"):
		m.Handle(c.prefix+"/", http.StripPrefix(c.prefix, serverHandler))
	default:
		m.Handle(c.prefix, http.StripPrefix(strings.TrimSuffix(c.prefix, "/"), serverHandler))
	}
	var listener net.Listener
	if c.listener == nil {
		var err error
		c.logger.InfoContext(ctx, "starting up with local httpd", "address", c.address)
		listener, err = net.Listen("tcp", c.address)
		if err != nil {
			return fmt.Errorf("couldn't listen to %s: %s", c.address, err.Error())
		}
	} else {
		listener = c.listener
		c.address = listener.Addr().String()
		c.logger.InfoContext(ctx, "starting up with local httpd", "address", listener.Addr().String())
	}
	if c.proxyProtocol {
		c.logger.InfoContext(ctx, "enables to PROXY protocol")
		listener = &proxyproto.Listener{Listener: listener}
	}
	srv := http.Server{Handler: m}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		c.logger.InfoContext(ctx, "shutting down local httpd", "address", c.address)
		shutdownCtx, timout := context.WithTimeout(context.Background(), 5*time.Second)
		defer timout()
		srv.Shutdown(shutdownCtx)
	}()
	if err := srv.Serve(listener); err != nil {
		if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			c.DebugContextWhenVarbose(ctx, "failed to start local httpd", "error", err)
			cancel()
			return err
		}
	}
	wg.Wait()
	return nil
}

func setupSQSPoller(ctx context.Context, workerHandler sqsEventLambdaHandlerFunc, c *runOptions, cancel context.CancelFunc) error {
	queueURL, client := c.SQSClientAndQueueURL()
	c.logger.InfoContext(ctx, "staring polling sqs queue", "queue", c.sqsQueueName, "on_memory_queue_mode", c.useInMemorySQS)
	poller := &sqsLongPollingService{
		sqsClient:           client,
		queueURL:            queueURL,
		maxNumberObMessages: int32(c.batchSize),
		waitTimeSeconds:     int32(c.pollingDuration.Seconds()),
		maxDeleteRetry:      3,
	}
	if c.logVarbose {
		poller.logger = c.logger.With(LogComponentAttributeKey, "sqs_poller")
	}
	if err := poller.Start(ctx, workerHandler); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		c.DebugContextWhenVarbose(ctx, "failed to start sqs poller", "error", err)
		cancel()
		return err
	}
	return nil
}

func setupWebSocketServer(ctx context.Context, serverHandler http.Handler, c *runOptions, cancel context.CancelFunc) error {
	var websocketListener net.Listener
	if c.websocketListener == nil {
		var err error
		c.logger.InfoContext(ctx, "starting up with local websocket server", "address", c.websocketAddress)
		websocketListener, err = net.Listen("tcp", c.websocketAddress)
		if err != nil {
			return fmt.Errorf("couldn't listen to %s: %s", c.address, err.Error())
		}
	} else {
		websocketListener = c.websocketListener
		c.websocketAddress = websocketListener.Addr().String()
		c.logger.InfoContext(ctx, "starting up with local websocket server", "address", websocketListener.Addr().String())
	}
	if c.websocketCallbackURL == "" {
		c.websocketCallbackURL = fmt.Sprintf("http://%s", c.websocketAddress)
	}
	bridgeHandler := NewWebsocketHTTPBridgeHandler(serverHandler)
	wsSrv := http.Server{Handler: bridgeHandler}
	go func() {
		<-ctx.Done()
		c.logger.InfoContext(ctx, "shutting down local websocket server", "address", c.address)
		shutdownCtx, timout := context.WithTimeout(context.Background(), 5*time.Second)
		defer timout()
		wsSrv.Shutdown(shutdownCtx)
	}()
	if err := wsSrv.Serve(websocketListener); err != nil {
		if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			c.DebugContextWhenVarbose(ctx, "failed to start local websocket server", "error", err)
			cancel()
			return err
		}
	}
	return nil
}

func setupFallbackLambdaHandler(ctx context.Context, lambdaFallbackHandler lambda.Handler, c *runOptions, cancel context.CancelFunc) error {
	c.logger.InfoContext(ctx, "staring fallback lambda handler, bypassing from stdin")
	decoder := jsonx.NewDecoder(c.stdin)
	for decoder.MoreWithContext(ctx) {
		var event json.RawMessage
		if err := decoder.DecodeWithContext(ctx, &event); err != nil {
			var jsonUnmarshalTypeError *json.UnmarshalTypeError
			var jsonSyntaxError *json.SyntaxError
			switch {
			case errors.Is(err, io.EOF),
				errors.Is(err, context.Canceled),
				errors.Is(err, context.DeadlineExceeded),
				errors.Is(err, io.ErrClosedPipe),
				errors.Is(err, io.ErrUnexpectedEOF):
				break
			case
				errors.As(err, &jsonUnmarshalTypeError),
				errors.As(err, &jsonSyntaxError):
				c.WarnContextWhenVarbose(ctx, "failed to decode event from stdin, reset decoder state", "error", err)
				decoder.SkipUntilValidToken()
				continue
			default:
				c.DebugContextWhenVarbose(ctx, "stop fallback lambda handler", "error", err, "type", fmt.Sprintf("%T", err))
				cancel()
				return err
			}
		}
		if _, err := lambdaFallbackHandler.Invoke(ctx, event); err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				c.DebugContextWhenVarbose(ctx, "failed to invoke fallback lambda handler", "error", err)
				cancel()
				return err
			}
			return nil
		}
	}
	return nil
}

// WorkerResponseWriter is a http.ResponseWriter for worker handler.
// when call worker handler, set this as http.ResponseWriter.
type WorkerResponseWriter struct {
	bytes.Buffer
	header     http.Header
	statusCode int
}

func NewWorkerResponseWriter() *WorkerResponseWriter {
	return &WorkerResponseWriter{
		header: http.Header{
			"Content-Type": []string{"text/plain; charset=utf-8"},
		},
		statusCode: http.StatusOK,
	}
}

func (w *WorkerResponseWriter) Header() http.Header {
	return w.header
}

func (w *WorkerResponseWriter) WriteHeader(code int) {
	w.statusCode = code
}

func (w *WorkerResponseWriter) Response(r *http.Request) *http.Response {
	return &http.Response{
		Proto:         r.Proto,
		ProtoMajor:    r.ProtoMajor,
		ProtoMinor:    r.ProtoMinor,
		Request:       r.Clone(context.Background()),
		ContentLength: int64(w.Len()),
		Status:        http.StatusText(w.statusCode),
		StatusCode:    w.statusCode,
		Header:        w.header,
		Body:          io.NopCloser(&w.Buffer),
	}
}

// LogComponentAttributeKey is a log/slog attribute key for canyon working component name [worker or server].
var LogComponentAttributeKey = "component"

func newWorkerHandler(mux http.Handler, c *runOptions) sqsEventLambdaHandlerFunc {
	logger := c.logger.With(slog.String(LogComponentAttributeKey, "worker"))
	serializer := c.serializer
	if s, ok := serializer.(LoggingableSerializer); ok && c.logVarbose {
		serializer = s.WithLogger(logger)
	}

	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if client, err := NewManagementAPIClientWithRequest(r, c.websocketCallbackURL); err == nil {
			ctx = EmbedWebsocketManagementAPIClient(ctx, client)
		} else {
			logger.DebugContext(ctx, "websocket management api client not found in request", "error", err)
		}
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
	visibilityTimeout, sqsClient := initializeSQSClient(c, logger)

	return func(ctx context.Context, event *events.SQSEvent) (*events.SQSEventResponse, error) {
		return processSQSEvent(ctx, event, wrappedHandler, serializer, c, logger, visibilityTimeout, sqsClient)
	}
}

func initializeSQSClient(c *runOptions, _ *slog.Logger) (time.Duration, SQSClient) {
	var once sync.Once
	var visibilityTimeout time.Duration = -1 * time.Second
	var sqsClient SQSClient

	once.Do(func() {
		queueURL, client := c.SQSClientAndQueueURL()
		sqsClient = client
		vt, err := getVisibilityTimeout(context.Background(), queueURL, client)
		if err != nil {
			var ae smithy.APIError
			if !errors.As(err, &ae) || ae.ErrorCode() != "AccessDeniedException" {
				c.cancel(err)
			}
		} else {
			visibilityTimeout = time.Duration(vt) * time.Second
		}
	})

	return visibilityTimeout, sqsClient
}

func processSQSEvent(
	ctx context.Context,
	event *events.SQSEvent,
	wrappedHandler http.Handler,
	serializer Serializer,
	c *runOptions,
	logger *slog.Logger,
	visibilityTimeout time.Duration,
	sqsClient SQSClient,
) (*events.SQSEventResponse, error) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var resp events.SQSEventResponse

	if visibilityTimeout > c.workerTimeoutMergin {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, visibilityTimeout-c.workerTimeoutMergin)
		defer cancel()
	}

	messageIds := make([]string, 0, len(event.Records))
	completed := make(map[string]bool, len(event.Records))
	changeMessageVisibilityBatchInput := &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(c.sqsQueueURL),
	}

	onFailure := func(record events.SQSMessage) {
		mu.Lock()
		defer mu.Unlock()
		resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{
			ItemIdentifier: record.MessageId,
		})
		completed[record.MessageId] = true
	}

	onSuccess := func(record events.SQSMessage) {
		mu.Lock()
		defer mu.Unlock()
		completed[record.MessageId] = true
	}

	setRetryAfter := func(record *events.SQSMessage, retryAfterSeconds int32) {
		mu.Lock()
		defer mu.Unlock()
		changeMessageVisibilityBatchInput.Entries = append(
			changeMessageVisibilityBatchInput.Entries,
			types.ChangeMessageVisibilityBatchRequestEntry{
				Id:                aws.String(record.MessageId),
				ReceiptHandle:     aws.String(record.ReceiptHandle),
				VisibilityTimeout: retryAfterSeconds,
			},
		)
	}

	for _, record := range event.Records {
		wg.Add(1)
		_logger := logger.With(slog.String("message_id", record.MessageId))
		messageIds = append(messageIds, record.MessageId)
		go func(record events.SQSMessage) {
			defer wg.Done()
			w := NewWorkerResponseWriter()
			r, err := serializer.Deserialize(ctx, &record)
			if err != nil {
				_logger.ErrorContext(ctx, "failed to restore request from sqs message", "error", err)
				onFailure(record)
				if retryAfter, ok := ErrorHasRetryAfter(err); ok {
					setRetryAfter(&record, retryAfter)
				}
				return
			}
			embededCtx := EmbedIsWorkerInContext(ctx, true)
			embededCtx = embedLoggerInContext(embededCtx, _logger)
			wrappedHandler.ServeHTTP(w, r.WithContext(embededCtx))
			workerResp := w.Response(r)
			if c.responseChecker.IsFailure(ctx, workerResp) {
				_logger.ErrorContext(ctx, "failed worker handler", "status_code", w.statusCode)
				onFailure(record)
				if retryAfter := workerResp.Header.Get("Retry-After"); retryAfter != "" {
					if retryAfterSeconds, err := strconv.ParseInt(retryAfter, 10, 32); err == nil {
						setRetryAfter(&record, int32(retryAfterSeconds))
					} else {
						_logger.WarnContext(ctx, "failed to parse Retry-After header", "error", err, "retry_after", retryAfter)
					}
				}
				return
			}
			onSuccess(record)
		}(record)
	}

	allDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allDone)
	}()

	select {
	case <-allDone:
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
			logger.WarnContext(ctx, "worker timeout exceeded", "timeout", visibilityTimeout)
			mu.Lock()
			for _, id := range messageIds {
				if !completed[id] {
					resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{
						ItemIdentifier: id,
					})
				}
			}
			mu.Unlock()
		}
	}

	if len(changeMessageVisibilityBatchInput.Entries) > 0 {
		if _, err := sqsClient.ChangeMessageVisibilityBatch(ctx, changeMessageVisibilityBatchInput); err != nil {
			logger.WarnContext(ctx, "failed to change message visibility", "error", err)
		}
	}

	return &resp, nil
}

func newServerHandler(mux http.Handler, c *runOptions) http.Handler {
	logger := c.logger.With(slog.String(LogComponentAttributeKey, "server"))
	serializer := c.serializer
	if s, ok := serializer.(LoggingableSerializer); ok && c.logVarbose {
		serializer = s.WithLogger(logger)
	}
	sender := newWorkerSender(mux, serializer, c)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if client, err := NewManagementAPIClientWithRequest(r, c.websocketCallbackURL); err == nil {
			ctx = EmbedWebsocketManagementAPIClient(ctx, client)
		} else {
			logger.DebugContext(ctx, "websocket management api client not found in request", "error", err)
		}
		ctx = embedLoggerInContext(ctx, logger)
		ctx = EmbedIsWorkerInContext(ctx, false)
		ctx = EmbedWorkerSenderInContext(ctx, sender)
		cloned, closer, err := BackupRequset(r.WithContext(ctx))
		if err != nil {
			logger.ErrorContext(ctx, "failed to backup request", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		defer func() {
			if err := closer(); err != nil {
				logger.ErrorContext(ctx, "failed to close backup request", "error", err)
			}
		}()
		mux.ServeHTTP(w, cloned)
	})
}

// LambdaHandlerFunc is a adapter for lambda.Handler.
type LambdaHandlerFunc func(ctx context.Context, event []byte) ([]byte, error)

// Invoke invokes LambdaHandlerFunc.
func (f LambdaHandlerFunc) Invoke(ctx context.Context, event []byte) ([]byte, error) {
	return f(ctx, event)
}

func newLambdaFallbackHandler(mux http.Handler, c *runOptions) lambda.Handler {
	if c.lambdaFallbackHandler == nil {
		return nil
	}
	logger := c.logger.With(slog.String(LogComponentAttributeKey, "fallback_handler"))
	serializer := c.serializer
	if s, ok := serializer.(LoggingableSerializer); ok && c.logVarbose {
		serializer = s.WithLogger(logger)
	}
	sender := newWorkerSender(mux, serializer, c)
	return LambdaHandlerFunc(func(ctx context.Context, event []byte) ([]byte, error) {
		ctx = embedLoggerInContext(ctx, logger)
		ctx = EmbedIsWorkerInContext(ctx, false)
		ctx = EmbedWorkerSenderInContext(ctx, sender)
		return c.lambdaFallbackHandler.Invoke(ctx, event)
	})
}

const DelayedSQSMessageID = "<delayed sqs message>"

func newWorkerSender(mux http.Handler, serializer Serializer, c *runOptions) WorkerSender {
	if c.useInMemorySQS && OnLambdaRuntime() {
		return WorkerSenderFunc(func(r *http.Request, opts *SendOptions) (string, error) {
			// recoall mux as worker
			ctx := EmbedIsWorkerInContext(r.Context(), true)
			w := NewWorkerResponseWriter()
			mux.ServeHTTP(w, r.WithContext(ctx))
			workerResp := w.Response(r)
			if c.responseChecker.IsFailure(ctx, workerResp) {
				return "", fmt.Errorf("failed similated worker handler: status_code=%d", w.statusCode)
			}
			return "in-memory-message", nil
		})
	}
	return defaultWorkerSender(serializer, c)
}

func defaultWorkerSender(serializer Serializer, c *runOptions) WorkerSender {
	return WorkerSenderFunc(func(r *http.Request, opts *SendOptions) (string, error) {
		queueURL, client := c.SQSClientAndQueueURL()
		ctx := r.Context()
		c.DebugContextWhenVarbose(ctx, "try sqs send message with http request", "method", r.Method, "path", r.URL.Path)
		input, err := serializer.Serialize(ctx, RestoreRequest(r))
		if err != nil {
			return "", fmt.Errorf("failed to serialize request: %w", err)
		}
		input.QueueUrl = aws.String(queueURL)
		if opts != nil {
			if len(opts.MessageAttributes) > 0 {
				if input.MessageAttributes == nil {
					input.MessageAttributes = make(map[string]types.MessageAttributeValue)
				}
				for k, v := range opts.MessageAttributes {
					input.MessageAttributes[k] = types.MessageAttributeValue{
						DataType:         aws.String(v.DataType),
						StringValue:      v.StringValue,
						BinaryValue:      v.BinaryValue,
						StringListValues: v.StringListValues,
						BinaryListValues: v.BinaryListValues,
					}
				}
			}
			if opts.MessageGroupID != nil {
				input.MessageGroupId = opts.MessageGroupID
			}
			if opts.DelaySeconds != nil {
				input.DelaySeconds = *opts.DelaySeconds
			}
		}
		// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html#SQS-SendMessage-request-DelaySeconds
		// > Valid values: 0 to 900. Maximum: 15 minutes.
		if input.DelaySeconds < 0 {
			input.DelaySeconds = 0
		}
		if input.DelaySeconds > 900 {
			if c.scheduler == nil {
				return "", fmt.Errorf("delay_seconds is too long: if need long delay, scheduler is required")
			}
			if err := c.scheduler.RegisterSchedule(ctx, input); err != nil {
				return "", fmt.Errorf("failed to register to scheduler: %w", err)
			}
			return DelayedSQSMessageID, nil
		}
		c.logger.DebugContext(ctx, "send sqs message", "queue_url", queueURL, "delay_seconds", input.DelaySeconds)
		output, err := client.SendMessage(ctx, input)
		if err != nil {
			return "", fmt.Errorf("failed to send sqs message: %w", err)
		}
		c.DebugContextWhenVarbose(ctx, "success sqs message sent with http request", "message_id", *output.MessageId, "queue", c.sqsQueueName)
		return *output.MessageId, nil
	})
}
