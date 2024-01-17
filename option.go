package canyon

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// WorkerResponseChecker is a interface for checking worker's http.Response.
type WorkerResponseChecker interface {
	// if return true, sqs message not deleted.
	IsFailure(ctx context.Context, resp *http.Response) bool
}

// WorkerResponseCheckerFunc is a func type for checking worker's http.Response.
type WorkerResponseCheckerFunc func(ctx context.Context, resp *http.Response) bool

func (f WorkerResponseCheckerFunc) IsFailure(ctx context.Context, resp *http.Response) bool {
	return f(ctx, resp)
}

// DefaultWorkerResponseChecker is a default WorkerResponseChecker.
// if http.Response status code is 2xx success, sqs message will be deleted.
// if http.Response status code is 3xx, 4xx, or 5xx sqs message will not be deleted.
var DefaultWorkerResponseChecker = WorkerResponseCheckerFunc(func(ctx context.Context, resp *http.Response) bool {
	return resp.StatusCode >= 300
})

type runOptions struct {
	mu sync.Mutex

	cancel                             context.CancelCauseFunc
	address                            string
	prefix                             string
	websocketAddress                   string
	websocketListener                  net.Listener
	websocketCallbackURL               string
	batchSize                          int
	logger                             *slog.Logger
	proxyProtocol                      bool
	sqsClient                          SQSClient
	backend                            Backend
	sqsQueueName                       string
	sqsQueueURL                        string
	useInMemorySQS                     bool
	inMemorySQSClientDLQ               io.Writer
	inMemorySQSClientMaxReceiveCount   int32
	inMemorySQSClientVisibilityTimeout time.Duration
	pollingDuration                    time.Duration
	listener                           net.Listener
	logVarbose                         bool
	responseChecker                    WorkerResponseChecker
	disableWorker                      bool
	disableServer                      bool
	disableWebsocket                   bool
	cleanupFuncs                       []func()
	lambdaFallbackHandler              lambda.Handler
	stdin                              io.Reader
	serializer                         Serializer
	scheduler                          Scheduler
	workerTimeoutMergin                time.Duration
}

func defaultRunConfig(cancel context.CancelCauseFunc, sqsQueueName string) *runOptions {
	c := &runOptions{
		batchSize:                          1,
		address:                            ":8080",
		prefix:                             "/",
		logger:                             slog.Default(),
		proxyProtocol:                      false,
		sqsQueueName:                       sqsQueueName,
		inMemorySQSClientDLQ:               io.Discard,
		inMemorySQSClientMaxReceiveCount:   3,
		inMemorySQSClientVisibilityTimeout: 30 * time.Second,
		pollingDuration:                    20 * time.Second,
		logVarbose:                         false,
		responseChecker:                    DefaultWorkerResponseChecker,
		disableWorker:                      false,
		disableServer:                      false,
		disableWebsocket:                   false,
		cleanupFuncs:                       []func(){},
		lambdaFallbackHandler:              nil,
		stdin:                              os.Stdin,
		serializer:                         NewDefaultSerializer(),
		workerTimeoutMergin:                1 * time.Second,
	}
	if cancel != nil {
		c.cancel = cancel
	} else {
		c.cancel = func(error) {}
	}
	if u, err := url.Parse(sqsQueueName); err == nil && u.Scheme != "" {
		c.sqsQueueURL = u.String()
	}
	return c
}

func (c *runOptions) SQSClientAndQueueURL() (string, SQSClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sqsClient == nil {
		c.DebugWhenVarbose("sqs client is not initialized, try to load default config")
		awsCfg, err := getDefaultAWSConfig(context.Background())
		if err != nil {
			c.DebugWhenVarbose("failed to load aws default config, set context cancel", "error", err)
			c.cancel(fmt.Errorf("load aws default config: %w", err))
			return "", sqs.New(sqs.Options{})
		}
		c.sqsClient = sqs.NewFromConfig(awsCfg)
	}
	if c.sqsQueueURL == "" {
		c.DebugWhenVarbose("sqs queue url is not initialized, try to get queue url")
		output, err := c.sqsClient.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{
			QueueName: &c.sqsQueueName,
		})
		if err != nil {
			c.DebugWhenVarbose("failed to get sqs queue url, set context cancel", "error", err)
			c.cancel(fmt.Errorf("get sqs queue url: %w", err))
			return "", c.sqsClient
		}
		c.sqsQueueURL = *output.QueueUrl
	}
	return c.sqsQueueURL, c.sqsClient
}

func (c *runOptions) DebugWhenVarbose(msg string, keysAndValues ...interface{}) {
	if c.logVarbose {
		c.logger.Debug(msg, keysAndValues...)
	}
}

func (c *runOptions) DebugContextWhenVarbose(ctx context.Context, msg string, keysAndValues ...interface{}) {
	if c.logVarbose {
		c.logger.DebugContext(ctx, msg, keysAndValues...)
	}
}

func (c *runOptions) InfoWhenVarbose(msg string, keysAndValues ...interface{}) {
	if c.logVarbose {
		c.logger.Info(msg, keysAndValues...)
	}
}

func (c *runOptions) InfoContextWhenVarbose(ctx context.Context, msg string, keysAndValues ...interface{}) {
	if c.logVarbose {
		c.logger.InfoContext(ctx, msg, keysAndValues...)
	}
}

func (c *runOptions) WarnContextWhenVarbose(ctx context.Context, msg string, keysAndValues ...interface{}) {
	if c.logVarbose {
		c.logger.WarnContext(ctx, msg, keysAndValues...)
	}
}

func (c *runOptions) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range c.cleanupFuncs {
		f()
	}
}

// Option is a Run() and RunWtihContext() option.
type Option func(*runOptions)

// WithContext returns a new Option that sets the local server listener.
// this option for testing. normally, you should not use this option.
// if production used, WithServerAddress() option.
func WithListener(listener net.Listener) Option {
	return func(c *runOptions) {
		c.listener = listener
	}
}

// WithServerAddress returns a new Option that sets the local server address.
// if you want to use proxy protocol, you should use this option.
func WithServerAddress(address string, prefix string) Option {
	return func(c *runOptions) {
		c.address = address
	}
}

// WithWorkerBatchSize returns a new Option that sets the local poller batch size.
// if run on AWS Lambda, ignore this option.
func WithWorkerBatchSize(batchSize int) Option {
	return func(c *runOptions) {
		if batchSize < 1 {
			batchSize = 1
		}
		c.batchSize = batchSize
	}
}

// WithSQSPollingDuration returns a new Option that sets the local poller polling duration.
// if run on AWS Lambda, ignore this option.
func WithSQSPollingDuration(pollingDuration time.Duration) Option {
	return func(c *runOptions) {
		pollingDuration = pollingDuration.Round(time.Second)
		if pollingDuration < 1 {
			pollingDuration = 1 * time.Second
		}
		c.pollingDuration = pollingDuration
	}
}

// WithLogger returns a new Option that sets the canyon logger.
// default is slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(c *runOptions) {
		c.logger = logger
	}
}

// WithProxyProtocol returns a new Option that enables to PROXY protocol.
// if you want to use proxy protocol, you should use this option.
// if run on AWS Lambda, ignore this option.
func WithProxyProtocol() Option {
	return func(c *runOptions) {
		c.proxyProtocol = true
	}
}

// WithSQSClient returns a new Option that sets the sqs client.
// this option for testing. normally, you should not use this option.
// default sqs client is loaded from aws default config.
func WithSQSClient(sqsClient SQSClient) Option {
	return func(c *runOptions) {
		c.sqsClient = sqsClient
		c.useInMemorySQS = false
	}
}

// WithVarbose returns a new Option that sets the canyon loggign verbose.
// this option for debugging canyon.
// canyon will output many debug log.
func WithVarbose() Option {
	return func(c *runOptions) {
		c.logVarbose = true
	}
}

// WithInMemoryQueue returns a new Option that sets the mode of on memory queue.
// if run on AWS Lambda, ignore this option.
// if set this option, canyon not used real AWS SQS.
// only used on memory queue.
// for local development.
func WithInMemoryQueue(visibilityTimeout time.Duration, maxReceiveCount int32, dlq io.Writer) Option {
	return func(c *runOptions) {
		c.useInMemorySQS = true
		c.inMemorySQSClientVisibilityTimeout = visibilityTimeout
		c.inMemorySQSClientMaxReceiveCount = maxReceiveCount
		if dlq == nil {
			dlq = io.Discard
		}
		c.inMemorySQSClientDLQ = dlq
	}
}

// WithWrokerResponseChecker returns a new Option that sets the worker response checker.
func WithWrokerResponseChecker(responseChecker WorkerResponseChecker) Option {
	return func(c *runOptions) {
		if responseChecker == nil {
			responseChecker = DefaultWorkerResponseChecker
		}
		c.responseChecker = responseChecker
	}
}

// WithDisableWorker returns a new Option that disable worker.
// if set this option, canyon not runnning worker.
func WithDisableWorker() Option {
	return func(c *runOptions) {
		c.disableWorker = true
	}
}

// WithDisableServer returns a new Option that disable server.
// if set this option, canyon not running server.
func WithDisableServer() Option {
	return func(c *runOptions) {
		c.disableServer = true
	}
}

// WithBackend returns a new Option
// if set this option, canyon using  backend.
// when send to sqs message, canyon upload request to any backend.
// and sqs message body set backend_url.
//
// SQS message body limit is 256KB. this option is useful for large request.
// for example S3Backend is request body upload to s3.
func WithBackend(b Backend) Option {
	return func(c *runOptions) {
		c.backend = b
	}
}

// WithCanyonEnv returns a new Option env swith default options.
// if env == "development", set varbose and in memory queue, temporary file backend.
// if env == "test", set in memory queue, in memory backend.
// otherwise, setup backend with os.Getenv("CANYON_BACKEND_URL").
//
//	if url is empty, not set backend.
//	if url schema is s3, set s3 backend.
//	if url schema is file, set file backend.
func WithCanyonEnv(envPrefix string) Option {
	return func(c *runOptions) {
		if envPrefix == "" {
			envPrefix = "CANYON_"
		}
		env := os.Getenv(envPrefix + "ENV")
		var useScheduler bool
		if str := os.Getenv(envPrefix + "SCHEDULER"); strings.EqualFold(str, "true") {
			useScheduler = true
		}
		switch strings.ToLower(env) {
		case "development":
			WithVarbose()(c)
			WithInMemoryQueue(30*time.Second, 3, nil)(c)
			var backendPath string
			if urlStr := os.Getenv(envPrefix + "BACKEND_URL"); urlStr != "" {
				if u, err := parseURL(urlStr); err == nil && u.Scheme == "file" {
					backendPath = u.Path
					c.InfoWhenVarbose("create file backend, canyon request body upload to directory", "path", backendPath)
				}
			}
			if backendPath == "" {
				tmp, err := os.MkdirTemp(os.TempDir(), "canon-*")
				if err != nil {
					c.cancel(fmt.Errorf("create temporary directory: %w", err))
					return
				}
				backendPath = tmp
				c.cleanupFuncs = append(c.cleanupFuncs, func() {
					c.InfoWhenVarbose("remove temporary file backend", "path", tmp)
					if err := os.RemoveAll(tmp); err != nil {
						c.logger.Error("failed to remove temporary directory", "path", tmp, "error", err)
					}
				})
				c.InfoWhenVarbose("create temporary file backend, canyon request body upload to temporary directory", "path", tmp)
			}
			c.DebugWhenVarbose("try to create file backend", "path", backendPath)
			b, err := NewFileBackend(backendPath)
			if err != nil {
				c.cancel(fmt.Errorf("create file backend: %w", err))
				return
			}
			WithBackend(b)(c)
			if useScheduler {
				WithScheduler(NewInMemoryScheduler(func() SQSClient {
					_, sqsClient := c.SQSClientAndQueueURL()
					return sqsClient
				}))(c)
			}
		case "test":
			WithInMemoryQueue(30*time.Second, 3, nil)(c)
			WithBackend(NewInMemoryBackend())(c)
			if useScheduler {
				WithScheduler(NewInMemoryScheduler(func() SQSClient {
					_, sqsClient := c.SQSClientAndQueueURL()
					return sqsClient
				}))(c)
			}
		default:
			env = "production"
			if urlStr := os.Getenv(envPrefix + "BACKEND_URL"); urlStr != "" {
				u, err := parseURL(urlStr)
				if err != nil {
					c.cancel(fmt.Errorf("parse backend url: %w", err))
					return
				}
				b, err := NewBackend(u)
				if err != nil {
					c.cancel(fmt.Errorf("create backend: %w", err))
					return
				}
				if appName := os.Getenv(envPrefix + "BACKEND_SAVE_APP_NAME"); appName != "" {
					if b, ok := b.(AppNameSetable); ok {
						b.SetAppName(appName)
					}
				}
				WithBackend(b)(c)
			}
			if useScheduler {
				scheduleNamePrefix := "canyon."
				if str := os.Getenv(envPrefix + "SCHEDULE_NAME_PREFIX"); str != "" {
					scheduleNamePrefix = str
				}
				s, err := NewEventBridgeScheduler(context.Background(), scheduleNamePrefix)
				if err != nil {
					c.cancel(fmt.Errorf("create eventbridge scheduler: %w", err))
					return
				}
				if groupName := os.Getenv(envPrefix + "SCHEDULE_GROUP_NAME"); groupName != "" {
					s.SetGroupName(groupName)
				}
				WithScheduler(s)(c)
			}
		}
		c.logger.Info("running canyon", "env", env)
	}
}

// WithLambdaFallbackHandler returns a new Option that sets the fallback lambda handler.
// if set this option, call fallback lambda handler when paylaod is not sqs message or http request.
func WithLambdaFallbackHandler(handler interface{}) Option {
	return func(c *runOptions) {
		c.lambdaFallbackHandler = lambda.NewHandler(handler)
	}
}

// WithStdin returns a new Option that sets the Stdin Stream reader.
// this option for testing. normally, you should not use this option.
// to fallback lambda handler test.
func WithStdin(stdin io.Reader) Option {
	return func(c *runOptions) {
		c.stdin = stdin
	}
}

// WithSerializer returns a new Option that sets the serializer.
// for http.Request Serializetion format change.
// if can use Backend implement BackendSerializer interface
func WithSerializer(serializer Serializer) Option {
	return func(c *runOptions) {
		c.serializer = serializer
	}
}

// WithScheduler returns a new Option that sets the scheduler.
// if set this option, canyon using scheduler.
// when send to delayed second over 900 seconds sqs message, canyon create schedule.
func WithScheduler(scheduler Scheduler) Option {
	return func(c *runOptions) {
		c.scheduler = scheduler
	}
}

// WithWorkerTimeoutMergin returns a new Option that sets the worker timeout mergin.
// if set this option, canyon worker timeout is visibility timeout - mergin.
// default mergin is 1 second.
// if visibility timeout is 30 seconds, worker timeout is 29 seconds.
func WithWorkerTimeoutMergin(mergin time.Duration) Option {
	return func(c *runOptions) {
		if mergin < 0 {
			mergin = 0
		}
		c.workerTimeoutMergin = mergin
	}
}

// WithWebsocketCallbackURL returns a new Option that sets the websocket callback url.
// if set this option, canyon websocket connections callback url.
func WithWebsocketCallbackURL(url string) Option {
	return func(c *runOptions) {
		c.websocketCallbackURL = url
	}
}

// WithWebsocketListener returns a new Option that sets the websocket listener.
// this option for testing. normally, you should not use this option.
// if production used, WithWebsocketAddress() option.
func WithWebsocketListener(listener net.Listener) Option {
	return func(c *runOptions) {
		c.websocketListener = listener
	}
}

// WithWebsocketAddress returns a new Option that sets the websocket address.
// if you want to use proxy protocol, you should use this option.
func WithWebsocketAddress(address string) Option {
	return func(c *runOptions) {
		c.websocketAddress = address
	}
}

// WithDisableWebsocket returns a new Option that disable websocket.
// if set this option, canyon not running websocket.
func WithDisableWebsocket() Option {
	return func(c *runOptions) {
		c.disableWebsocket = true
	}
}
