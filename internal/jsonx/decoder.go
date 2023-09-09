package jsonx

import (
	"context"
	"encoding/json"
	"io"
	"sync"
)

type Decoder struct {
	mu sync.RWMutex
	json.Decoder
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		Decoder: *json.NewDecoder(r),
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
