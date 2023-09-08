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
	"os"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/config"
	sdklambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/fujiwara/ridge"
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
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	if c.disableWorker && c.disableServer {
		return errors.New("both worker and server are disabled")
	}
	if err := runWithContext(ctx, mux, c); err != nil {
		cancel(err)
		return err
	}
	return context.Cause(ctx)
}

var (
	enableReportBatchItemFailures  map[string]bool = make(map[string]bool)
	onceCheckFunctionResponseTypes sync.Once
)

func runWithContext(ctx context.Context, mux http.Handler, c *runOptions) error {
	workerHandler := newWorkerHandler(mux, c)
	serverHandler := newServerHandler(mux, c)
	if strings.HasPrefix(os.Getenv("AWS_EXECUTION_ENV"), "AWS_Lambda") || os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		lambda.StartWithOptions(
			func(ctx context.Context, event json.RawMessage) (interface{}, error) {
				var p eventPayload
				if err := json.Unmarshal(event, &p); err != nil {
					return nil, err
				}
				if p.IsSQSEvent && !c.disableWorker {
					onceCheckFunctionResponseTypes.Do(func() {
						defer func() {
							for k, v := range enableReportBatchItemFailures {
								if v {
									c.logger.Info("enable report batch item failures", "event_source_arn", k)
								}
							}
						}()

						awsCfg, err := config.LoadDefaultConfig(ctx)
						if err != nil {
							return
						}
						lambdaClient := sdklambda.NewFromConfig(awsCfg)
						lc, ok := lambdacontext.FromContext(ctx)
						if !ok {
							c.logger.Warn("missing lambda context for check function response types")
							return
						}
						p := sdklambda.NewListEventSourceMappingsPaginator(lambdaClient, &sdklambda.ListEventSourceMappingsInput{
							FunctionName: &lc.InvokedFunctionArn,
						})
						for p.HasMorePages() {
							list, err := p.NextPage(ctx)
							if err != nil {
								c.logger.Warn("failed to list event source mappings", "error", err)
								return
							}
							for _, m := range list.EventSourceMappings {
								if m.EventSourceArn == nil {
									continue
								}
								if !strings.HasPrefix(*m.EventSourceArn, "arn:aws:sqs:") {
									continue
								}
								for _, v := range m.FunctionResponseTypes {
									if v == lambdatypes.FunctionResponseTypeReportBatchItemFailures {
										enableReportBatchItemFailures[*m.EventSourceArn] = true
									}
								}
							}
						}
					})
					resp, err := workerHandler(ctx, p.SQSEvent)
					if err != nil {
						return nil, err
					}
					if len(resp.BatchItemFailures) == 0 {
						return resp, nil
					}
					if !enableReportBatchItemFailures[p.SQSEvent.Records[0].EventSourceARN] {
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
				return nil, errors.New("unsupported event")
			},
			lambda.WithContext(ctx),
		)
		return nil
	}
	if c.useFakeSQSRunOnLocal {
		c.logger.Info("enable on memory queue", "visibility_timeout", c.fakeSQSClientVisibilityTimeout, "max_receive_count", c.fakeSQSClientMaxReceiveCount)
		fakeClient := &fakeSQSClient{
			visibilityTimeout: c.fakeSQSClientVisibilityTimeout,
			maxReceiveCount:   int(c.fakeSQSClientMaxReceiveCount),
			dlq:               json.NewEncoder(c.fakeSQSClientDLQ),
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
		go func() {
			wg.Add(1)
			defer func() {
				c.logger.InfoContext(cctx, "shutting down sqs poller", "queue", c.sqsQueueName)
				wg.Done()
			}()
			queueURL, client := c.SQSClientAndQueueURL()
			c.logger.InfoContext(cctx, "staring polling sqs queue", "queue", c.sqsQueueName, "on_memory_queue_mode", c.useFakeSQSRunOnLocal)
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
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		ctx = embedLoggerInContext(ctx, logger)
		ctx = EmbedIsWorkerInContext(ctx, false)
		ctx = EmbedSQSMessageSenderInContext(ctx,
			SQSMessageSenderFunc(func(r *http.Request, m MessageAttributes) (string, error) {
				queueURL, client := c.SQSClientAndQueueURL()
				l := Logger(r)
				if c.logVarbose {
					l.DebugContext(r.Context(), "try sqs send message with http request", "method", r.Method, "path", r.URL.Path)
				}
				input, err := serializer.NewSendMessageInput(ctx, queueURL, r, m)
				if err != nil {
					return "", fmt.Errorf("failed to create sqs message: %w", err)
				}
				output, err := client.SendMessage(r.Context(), input)
				if err != nil {
					return "", fmt.Errorf("failed to send sqs message: %w", err)
				}
				if c.logVarbose {
					l.DebugContext(r.Context(), "success sqs message sent with http request", "message_id", *output.MessageId, "queue", c.sqsQueueName)
				}
				return *output.MessageId, nil
			}),
		)
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
