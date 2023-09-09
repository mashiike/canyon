package jsonx

import (
	"context"
	"encoding/json"
	"io"
	"sync"
)

type Decoder struct {
	mu sync.Mutex
	json.Decoder
	r io.Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		Decoder: *json.NewDecoder(r),
		r:       r,
	}
}

func (d *Decoder) Decode(v interface{}) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.Decoder.Decode(v)
}

func (d *Decoder) DecodeWithContext(ctx context.Context, v interface{}) error {
	errCh := make(chan error, 1)
	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		err := d.Decoder.Decode(v)
		errCh <- err
		close(errCh)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (d *Decoder) More() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.Decoder.More()
}

func (d *Decoder) MoreWithContext(ctx context.Context) bool {
	resultCh := make(chan bool, 1)
	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		resultCh <- d.Decoder.More()
		close(resultCh)
	}()
	select {
	case <-ctx.Done():
		return false
	case more := <-resultCh:
		return more
	}
}

func (d *Decoder) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Decoder = *json.NewDecoder(d.r)
}
