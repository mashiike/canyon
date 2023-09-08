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

	"github.com/aws/aws-sdk-go-v2/config"
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

	cancel                         context.CancelCauseFunc
	address                        string
	prefix                         string
	batchSize                      int
	logger                         *slog.Logger
	proxyProtocol                  bool
	sqsClient                      SQSClient
	backend                        Backend
	sqsQueueName                   string
	sqsQueueURL                    string
	useFakeSQSRunOnLocal           bool
	fakeSQSClientDLQ               io.Writer
	fakeSQSClientMaxReceiveCount   int32
	fakeSQSClientVisibilityTimeout time.Duration
	pollingDuration                time.Duration
	listener                       net.Listener
	logVarbose                     bool
	responseChecker                WorkerResponseChecker
	disableWorker                  bool
	disableServer                  bool
}

func (c *runOptions) SQSClientAndQueueURL() (string, SQSClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sqsClient == nil {
		c.DebugWhenVarbose("sqs client is not initialized, try to load default config")
		awsCfg, err := config.LoadDefaultConfig(context.Background())
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

// Option is a Run() and RunWtihContext() option.
type Option func(*runOptions)

func defaultRunConfig(cancel context.CancelCauseFunc, sqsQueueName string) *runOptions {
	c := &runOptions{
		batchSize:                      1,
		address:                        ":8080",
		prefix:                         "/",
		logger:                         slog.Default(),
		proxyProtocol:                  false,
		sqsQueueName:                   sqsQueueName,
		fakeSQSClientDLQ:               io.Discard,
		fakeSQSClientMaxReceiveCount:   3,
		fakeSQSClientVisibilityTimeout: 30 * time.Second,
		pollingDuration:                20 * time.Second,
		logVarbose:                     false,
		responseChecker:                DefaultWorkerResponseChecker,
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
		c.useFakeSQSRunOnLocal = false
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
func WithInMemoryQueue(visibilityTimeout time.Duration, maxReceiveCount int64, dlq io.Writer) Option {
	return func(c *runOptions) {
		c.useFakeSQSRunOnLocal = true
		if dlq == nil {
			c.fakeSQSClientDLQ = io.Discard
		}
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
		opts := []Option{}
		if envPrefix == "" {
			envPrefix = "CANYON_"
		}
		env := os.Getenv(envPrefix + "ENV")
		switch strings.ToLower(env) {
		case "development":
			opts = append(opts, WithVarbose())
			opts = append(opts, WithInMemoryQueue(30*time.Second, 3, nil))
			tmp, err := os.MkdirTemp(os.TempDir(), "canon-*")
			if err != nil {
				c.cancel(fmt.Errorf("create temporary directory: %w", err))
				return
			}
			b, err := NewFileBackend(tmp)
			if err != nil {
				c.cancel(fmt.Errorf("create temporary file backend: %w", err))
				return
			}
			c.logger.Info("create temporary file backend, canyon request body upload to temporary directory", "path", tmp)
			opts = append(opts, WithBackend(b))
		case "test":
			opts = append(opts, WithInMemoryQueue(30*time.Second, 3, nil))
			opts = append(opts, WithBackend(NewInMemoryBackend()))
		default:
			if urlStr := os.Getenv(envPrefix + "BACKEND_URL"); urlStr != "" {
				u, err := url.Parse(urlStr)
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
				opts = append(opts, WithBackend(b))
			}
		}
		c.logger.Info("running canyon", "env", env)
		for _, opt := range opts {
			opt(c)
		}
	}
}
