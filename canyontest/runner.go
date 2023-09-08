package canyontest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/mashiike/canyon"
)

type Runner struct {
	URL      string // base URL of form http://ipaddr:port with no trailing slash
	Listener net.Listener

	closed   bool
	cancel   context.CancelCauseFunc
	wg       sync.WaitGroup
	runError error
}

func NewRunner(mux http.Handler, _opts ...canyon.Option) *Runner {
	ctx, cancel := context.WithCancelCause(context.Background())
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprintf("listen failed: %s", err))
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		panic(fmt.Sprintf("split host port failed: %s", err))
	}
	r := &Runner{
		URL:      fmt.Sprintf("http://127.0.0.1:%s", port),
		Listener: listener,
		cancel:   cancel,
	}
	opts := []canyon.Option{
		canyon.WithListener(listener),
		canyon.WithInMemoryQueue(
			30*time.Second, // on memory queue's default visibility timeout
			10,             // on memory queue's default max receive count,
			os.Stdout,      // if exceed max receive count, message will be sent to stdout as json
		),
		canyon.WithBackend(canyon.NewInMemoryBackend()),
	}
	if len(_opts) > 0 {
		opts = append(opts, _opts...)
	}
	r.wg.Add(1)
	go func() {
		defer func() {
			r.wg.Done()
			r.Close()
		}()
		if err := canyon.RunWithContext(ctx, "canyon", mux, opts...); err != nil {
			r.runError = err
		}
	}()
	return r
}

func (r *Runner) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.Listener.Close()
	r.wg.Wait()
	return nil
}

func (r *Runner) RunError() error {
	return r.runError
}