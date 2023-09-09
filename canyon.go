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
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
	if isLambda() {
		lambda.StartWithOptions(
			func(ctx context.Context, event json.RawMessage) (interface{}, error) {
				var p eventPayload
				if err := json.Unmarshal(event, &p); err != nil {
					if c.lambdaFallbackHandler != nil {
						return c.lambdaFallbackHandler.Invoke(ctx, event)
					}
					return nil, err
				}
				if p.IsSQSEvent && !c.disableWorker {
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
				if p.IsHTTPEvent && !c.disableServer {
					r := p.Request.WithContext(ctx)
					w := ridge.NewResponseWriter()
					serverHandler.ServeHTTP(w, r)
					return w.Response(), nil
				}
				if c.lambdaFallbackHandler != nil {
					return c.lambdaFallbackHandler.Invoke(ctx, event)
				}
				return nil, errors.New("unsupported event")
			},
			lambda.WithContext(ctx),
		)
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
	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if !c.disableServer {
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
	if c.lambdaFallbackHandler != nil {
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
				if _, err := c.lambdaFallbackHandler.Invoke(cctx, event); err != nil {
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
	serializer := NewSerializer(c.backend)
	if c.logVarbose {
		serializer.SetLogger(logger)
	}
	return func(ctx context.Context, event *events.SQSEvent) (*events.SQSEventResponse, error) {
		var mu sync.Mutex
		var wg sync.WaitGroup
		var resp events.SQSEventResponse
		for _, record := range event.Records {
			wg.Add(1)
			_logger := logger.With(slog.String("message_id", record.MessageId))
			go func(record events.SQSMessage) {
				defer wg.Done()
				w := NewWorkerResponseWriter()
				r, err := serializer.Deserialize(ctx, record)
				if err != nil {
					_logger.ErrorContext(
						ctx,
						"failed to restore request from sqs message",
						"error", err,
					)
					mu.Lock()
					resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{
						ItemIdentifier: record.MessageId,
					})
					mu.Unlock()
					return
				}
				embededCtx := EmbedIsWorkerInContext(ctx, true)
				embededCtx = embedLoggerInContext(embededCtx, _logger)
				mux.ServeHTTP(w, r.WithContext(embededCtx))
				workerResp := w.Response(r)
				if c.responseChecker.IsFailure(ctx, workerResp) {
					_logger.ErrorContext(
						ctx,
						"failed worker handler",
						"status_code", w.statusCode,
					)
					mu.Lock()
					resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{
						ItemIdentifier: record.MessageId,
					})
					mu.Unlock()
				}

			}(record)
		}
		wg.Wait()
		return &resp, nil
	}
}

func newServerHandler(mux http.Handler, c *runOptions) http.Handler {
	logger := c.logger.With(slog.String(LogComponentAttributeKey, "server"))
	serializer := NewSerializer(c.backend)
	if c.logVarbose {
		serializer.SetLogger(logger)
	}
	var sender SQSMessageSender
	if isLambda() && c.useInMemorySQS {
		sender = SQSMessageSenderFunc(func(r *http.Request, m MessageAttributes) (string, error) {
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
		sender = SQSMessageSenderFunc(func(r *http.Request, m MessageAttributes) (string, error) {
			queueURL, client := c.SQSClientAndQueueURL()
			l := Logger(r)
			if c.logVarbose {
				l.DebugContext(r.Context(), "try sqs send message with http request", "method", r.Method, "path", r.URL.Path)
			}
			ctx := r.Context()
			ctx = embedLoggerInContext(ctx, logger)
			input, err := serializer.NewSendMessageInput(ctx, queueURL, r, m)
			if err != nil {
				return "", fmt.Errorf("failed to create sqs message: %w", err)
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
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		ctx = embedLoggerInContext(ctx, logger)
		ctx = EmbedIsWorkerInContext(ctx, false)
		ctx = EmbedSQSMessageSenderInContext(ctx, sender)
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

func SendToWorker(r *http.Request, messageAttrs map[string]types.MessageAttributeValue) (string, error) {
	sqsMessageSender := sqsMessageSenderFromContext(r.Context())
	if sqsMessageSender == nil {
		return "", errors.New("sqs message sender is not set: may be worker or not running with canyon")
	}
	return sqsMessageSender.SendMessage(r, messageAttrs)
}
