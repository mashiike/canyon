package canyon

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
)

// JSONSerializableRequest is a request that can be serialized to JSON.
type JSONSerializableRequest struct {
	Method        string              `json:"method,omitempty"`
	Header        map[string][]string `json:"header,omitempty"`
	Body          string              `json:"body,omitempty"`
	ContentLength int64               `json:"content_length,omitempty"`
	RemoteAddr    string              `json:"remote_addr,omitempty"`
	Host          string              `json:"host,omitempty"`
	RequestURI    string              `json:"request_uri,omitempty"`
	URL           string              `json:"url,omitempty"`
}

// NewJSONSerializableRequest creates JSONSerializableRequest from http.Request.
func NewJSONSerializableRequest(r *http.Request) (*JSONSerializableRequest, error) {
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return &JSONSerializableRequest{
		Method:        r.Method,
		Header:        r.Header,
		Body:          base64.StdEncoding.EncodeToString(bs),
		ContentLength: r.ContentLength,
		RemoteAddr:    r.RemoteAddr,
		Host:          r.Host,
		RequestURI:    r.RequestURI,
		URL:           r.URL.String(),
	}, nil
}

// Desirialize desirializes JSONSerializableRequest to http.Request.
func (r *JSONSerializableRequest) Desirialize() (*http.Request, error) {
	bs, err := base64.StdEncoding.DecodeString(r.Body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(r.Method, r.URL, bytes.NewReader(bs))
	if err != nil {
		return nil, err
	}
	for k, v := range r.Header {
		req.Header.Del(k)
		for _, s := range v {
			req.Header.Add(k, s)
		}
	}
	req.RemoteAddr = r.RemoteAddr
	req.Host = r.Host
	req.RequestURI = r.RequestURI
	return req, nil
}
