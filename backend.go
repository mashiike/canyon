package canyon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

// InMemoryBackend is a backend for storing request body in memory.
type InMemoryBackend struct {
	mu                sync.RWMutex
	storedRequestBody map[string][]byte
}

// NewInMemoryBackend returns new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		storedRequestBody: make(map[string][]byte),
	}
}

// SaveRequestBody stores request body in memory.
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

// LoadRequestBody loads request body from memory.
func (b *InMemoryBackend) LoadRequestBody(ctx context.Context, backendURL *url.URL) (io.ReadCloser, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	bs, ok := b.storedRequestBody[backendURL.String()]
	if !ok {
		return nil, fmt.Errorf("backend url not found: %s", backendURL.String())
	}
	return io.NopCloser(bytes.NewReader(bs)), nil
}

// FileBackend is a backend for storing request body local file system
type FileBackend struct {
	path string
}

// NewFileBackend returns new FileBackend.
func NewFileBackend(path string) (*FileBackend, error) {
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			parentPath := filepath.Dir(path)
			if parentSt, err := os.Stat(parentPath); err == nil && parentSt.IsDir() {
				if err := os.MkdirAll(path, 0755); err != nil {
					return nil, fmt.Errorf("failed to create directory: %w", err)
				}
			} else {
				return nil, fmt.Errorf("failed to stat parent path: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to stat path: %w", err)
		}
	} else {
		if !st.IsDir() {
			return nil, fmt.Errorf("path is not directory")
		}
	}
	if !filepath.IsAbs(path) {
		path, err = filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path: %w", err)
		}
	}
	return &FileBackend{
		path: path,
	}, nil
}

// SaveRequestBody stores request body in local file system.
func (b *FileBackend) SaveRequestBody(ctx context.Context, req *http.Request) (*url.URL, error) {
	bs, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}
	backendURL := &url.URL{
		Scheme: "file",
		Path:   fmt.Sprintf("%s/%s%s", b.path, uuid.New().String(), getExtension(req.Header.Get("Content-Type"))),
	}
	if err := os.WriteFile(backendURL.Path, bs, 0644); err != nil {
		return nil, fmt.Errorf("failed to write file: %w", err)
	}
	return backendURL, nil
}

// LoadRequestBody loads request body from local file system.
func (b *FileBackend) LoadRequestBody(ctx context.Context, backendURL *url.URL) (io.ReadCloser, error) {
	fp, err := os.Open(backendURL.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, fp); err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return io.NopCloser(&buf), nil
}
