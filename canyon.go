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
	if isLambda() {
		lambdaOptions := make([]lambda.Option, len(c.lambdaOptions), len(c.lambdaOptions)+1)
		copy(lambdaOptions, c.lambdaOptions)
		lambdaOptions = append(lambdaOptions, lambda.WithContext(ctx))
		lambdaHandler := lambda.NewHandlerWithOptions(func(ctx context.Context, event json.RawMessage) (interface{}, error) {
			var p eventPayload
			if err := json.Unmarshal(event, &p); err != nil {
				if lambdaFallbackHandler != nil {
					return lambdaFallbackHandler.Invoke(ctx, event)
				}
				return nil, err
			}
			if p.IsSQSEvent {
				if len(p.SQSEvent.Records) > 1 {
					onceCheckFunctionResponseTypes.Do(func() {
						checkFunctionResponseTypes(ctx, c)
					})
				}

				resp, err := workerHandler(ctx, p.SQSEvent)
				if err != nil {
					return nil, err
				}
				if len(resp.BatchItemFailures) == 0 {
					return resp, nil
				}
				if len(p.SQSEvent.Records) == 1 {
					return resp, fmt.Errorf("failed processing record(message_id=%s)", p.SQSEvent.Records[0].MessageId)
				}
				if !isEnableReportBatchItemFailures(ctx, p.SQSEvent.Records[0].EventSourceARN) {
					return resp, fmt.Errorf("failed processing %d records", len(resp.BatchItemFailures))
				}
				return resp, nil
			}
			if p.IsHTTPEvent {
				r := p.Request.WithContext(ctx)
				w := ridge.NewResponseWriter()
				serverHandler.ServeHTTP(w, r)
				return w.Response(), nil
			}
			if p.IsWebsocketProxyEvent {
				r := p.Request.WithContext(ctx)
				w := ridge.NewResponseWriter()
				serverHandler.ServeHTTP(w, r)
				return w.Response(), nil
			}

			if lambdaFallbackHandler != nil {
				return lambdaFallbackHandler.Invoke(ctx, event)
			}
			return nil, errors.New("unsupported event")
		}, lambdaOptions...)
		for _, middleware := range c.lambdaMiddlewares {
			lambdaHandler = middleware(lambdaHandler)
		}
		lambda.Start(lambdaHandler)
		return nil
	}
	if c.useInMemorySQS {
		c.logger.Info("enable in memory queue", "visibility_timeout", c.inMemorySQSClientVisibilityTimeout.String(), "max_receive_count", c.inMemorySQSClientMaxReceiveCount)
		fakeClient := &inMemorySQSClient{
			visibilityTimeout: c.inMemorySQSClientVisibilityTimeout,
			maxReceiveCount:   int(c.inMemorySQSClientMaxReceiveCount),
			dlq:               json.NewEncoder(c.inMemorySQSClientDLQ),
		}
		if c.logVarbose {
			fakeClient.logger = c.logger.With(LogComponentAttributeKey, "fake_sqs")
		}
		c.sqsClient = fakeClient
	}

	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if !c.disableServer {
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
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-cctx.Done()
			c.logger.InfoContext(cctx, "shutting down local httpd", "address", c.address)
			shutdownCtx, timout := context.WithTimeout(context.Background(), 5*time.Second)
			defer timout()
			srv.Shutdown(shutdownCtx)
		}()
		go func() {
			defer wg.Done()
			if err := srv.Serve(listener); err != nil {
				if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					c.DebugContextWhenVarbose(cctx, "failed to start local httpd", "error", err)
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					cancel()
				}
			}
		}()
	}
	if !c.disableWorker {
		wg.Add(1)
		go func() {
			defer func() {
				c.logger.InfoContext(cctx, "shutting down sqs poller", "queue", c.sqsQueueName)
				wg.Done()
			}()
			queueURL, client := c.SQSClientAndQueueURL()
			c.logger.InfoContext(cctx, "staring polling sqs queue", "queue", c.sqsQueueName, "on_memory_queue_mode", c.useInMemorySQS)
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
			if err := poller.Start(cctx, workerHandler); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				c.DebugContextWhenVarbose(cctx, "failed to start sqs poller", "error", err)
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				cancel()
			}
		}()
	}
	if !c.disableWebsocket && (c.websocketListener != nil || c.websocketAddress != "") {
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
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-cctx.Done()
			c.logger.InfoContext(cctx, "shutting down local websocket server", "address", c.address)
			shutdownCtx, timout := context.WithTimeout(context.Background(), 5*time.Second)
			defer timout()
			wsSrv.Shutdown(shutdownCtx)
		}()
		go func() {
			defer wg.Done()
			if err := wsSrv.Serve(websocketListener); err != nil {
				if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					c.DebugContextWhenVarbose(cctx, "failed to start local websocket server", "error", err)
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					cancel()
				}
			}
		}()
	}
	if lambdaFallbackHandler != nil {
		wg.Add(1)
		go func() {
			defer func() {
				c.logger.InfoContext(cctx, "shutting down fallback lambda handler")
				wg.Done()
			}()
			c.logger.InfoContext(cctx, "staring fallback lambda handler, bypassing from stdin")
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
						c.WarnContextWhenVarbose(cctx, "failed to decode event from stdin, reset decoder state", "error", err)
						decoder.SkipUntilValidToken()
						continue
					default:
						c.DebugContextWhenVarbose(cctx, "stop fallback lambda handler", "error", err, "type", fmt.Sprintf("%T", err))
						mu.Lock()
						errs = append(errs, err)
						mu.Unlock()
					}
					cancel()
					return
				}
				if _, err := lambdaFallbackHandler.Invoke(cctx, event); err != nil {
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						c.DebugContextWhenVarbose(cctx, "failed to invoke fallback lambda handler", "error", err)
						mu.Lock()
						errs = append(errs, err)
						mu.Unlock()
						cancel()
					}
					return
				}
			}
		}()
	}
	wg.Wait()
	errCtxCarsed := context.Cause(ctx)
	if len(errs) > 0 {
		if errCtxCarsed != nil {
			for _, err := range errs {
				if errors.Is(err, errCtxCarsed) {
					return errors.Join(errs...)
				}
			}
			errs = append(errs, errCtxCarsed)
		}
		return errors.Join(errs...)
	}
	return errCtxCarsed
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
	wraped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if client, err := NewManagementAPIClientWithRequest(r, c.websocketCallbackURL); err == nil {
			ctx = EmbedWebsocketManagementAPIClient(ctx, client)
		} else {
			logger.DebugContext(ctx, "websocket management api client not found in request", "error", err)
		}
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
	var once sync.Once
	var visibilityTimeout time.Duration = -1 * time.Second
	var sqsClient SQSClient
	return func(ctx context.Context, event *events.SQSEvent) (*events.SQSEventResponse, error) {
		once.Do(func() {
			queueURL, client := c.SQSClientAndQueueURL()
			sqsClient = client
			vt, err := getVisibilityTimeout(ctx, queueURL, client)
			if err != nil {
				var ae smithy.APIError
				if !errors.As(err, &ae) {
					c.cancel(err)
					return
				}
				if ae.ErrorCode() != "AccessDeniedException" {
					return
				}
			}
			visibilityTimeout = time.Duration(vt) * time.Second
		})
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
					VisibilityTimeout: retryAfterSeconds, // after adding current visibility timeout
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
					_logger.ErrorContext(
						ctx,
						"failed to restore request from sqs message",
						"error", err,
					)
					onFailure(record)
					retryAfter, ok := ErrorHasRetryAfter(err)
					if !ok {
						return
					}
					setRetryAfter(&record, retryAfter)
					return
				}
				embededCtx := EmbedIsWorkerInContext(ctx, true)
				embededCtx = embedLoggerInContext(embededCtx, _logger)
				wraped.ServeHTTP(w, r.WithContext(embededCtx))
				workerResp := w.Response(r)
				if c.responseChecker.IsFailure(ctx, workerResp) {
					_logger.ErrorContext(
						ctx,
						"failed worker handler",
						"status_code", w.statusCode,
					)
					onFailure(record)
					retryAfter := workerResp.Header.Get("Retry-After")
					if retryAfter == "" {
						return
					}
					retryAfterSeconds, err := strconv.ParseInt(retryAfter, 10, 32)
					if err != nil {
						_logger.WarnContext(
							ctx,
							"failed to parse Retry-After header",
							"error", err,
							"retry_after", retryAfter,
						)
						return
					}
					setRetryAfter(&record, int32(retryAfterSeconds))
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
					if completed[id] {
						continue
					}
					resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{
						ItemIdentifier: id,
					})
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
		defer closer()
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
	if isLambda() && c.useInMemorySQS {
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
	} else {
		return WorkerSenderFunc(func(r *http.Request, opts *SendOptions) (string, error) {
			queueURL, client := c.SQSClientAndQueueURL()
			l := Logger(r)
			if c.logVarbose {
				l.DebugContext(r.Context(), "try sqs send message with http request", "method", r.Method, "path", r.URL.Path)
			}
			ctx := r.Context()
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
			output, err := client.SendMessage(ctx, input)
			if err != nil {
				return "", fmt.Errorf("failed to send sqs message: %w", err)
			}
			if c.logVarbose {
				l.DebugContext(r.Context(), "success sqs message sent with http request", "message_id", *output.MessageId, "queue", c.sqsQueueName)
			}
			return *output.MessageId, nil
		})
	}
}
