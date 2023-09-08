package canyon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/google/uuid"
)

type InMemoryBackend struct {
	mu                sync.RWMutex
	storedRequestBody map[string][]byte
}

func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		storedRequestBody: make(map[string][]byte),
	}
}

func (b *InMemoryBackend) SaveRequestBody(ctx context.Context, req *http.Request) (*url.URL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.storedRequestBody == nil {
		b.storedRequestBody = make(map[string][]byte)
	}
	bs, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}
	backendURL, err := url.Parse(fmt.Sprintf("in-memory://canyon/%s", uuid.New().String()))
	if err != nil {
		return nil, fmt.Errorf("failed to parse backend url: %w", err)
	}
	b.storedRequestBody[backendURL.String()] = bs
	return backendURL, nil
}

func (b *InMemoryBackend) LoadRequestBody(ctx context.Context, backendURL *url.URL) (io.ReadCloser, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	bs, ok := b.storedRequestBody[backendURL.String()]
	if !ok {
		return nil, fmt.Errorf("backend url not found: %s", backendURL.String())
	}
	return io.NopCloser(bytes.NewReader(bs)), nil
}
