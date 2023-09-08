package canyon

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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

type S3UploadedRequest struct {
	*JSONSerializableRequest
	bucketName       string
	objectKey        string
	Uploader         *manager.Uploader      `json:"-"`
	Downloader       *manager.Downloader    `json:"-"`
	HeadObjectClient s3.HeadObjectAPIClient `json:"-"`
}

func (r *S3UploadedRequest) SetS3Client(s3Client S3Client) {
	r.Uploader = manager.NewUploader(s3Client)
	r.Downloader = manager.NewDownloader(s3Client)
	r.HeadObjectClient = s3Client
}

func (r *S3UploadedRequest) Upload(ctx context.Context, s3URL *url.URL) error {
	if !isS3URL(s3URL) {
		return fmt.Errorf("invalid s3 url: %s", s3URL.String())
	}
	if r.Uploader == nil {
		return fmt.Errorf("uploader is not set")
	}
	bs, err := json.Marshal(r.JSONSerializableRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal json: %w", err)
	}
	// upload body
	objectKey := strings.TrimLeft(s3URL.Path, "/")
	_, err = r.Uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s3URL.Host),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(bs),
	})
	if err != nil {
		return fmt.Errorf("failed to upload request: %w", err)
	}
	r.bucketName = s3URL.Host
	r.objectKey = objectKey
	return nil
}

func (r *S3UploadedRequest) IsUploaded() bool {
	return r.bucketName != ""
}

func (r *S3UploadedRequest) MarshalJSON() ([]byte, error) {
	if !r.IsUploaded() {
		return json.Marshal(r.JSONSerializableRequest)
	}
	return json.Marshal(struct {
		BucketName string `json:"bucket_name,omitempty"`
		ObjectKey  string `json:"object_key,omitempty"`
	}{
		BucketName: r.bucketName,
		ObjectKey:  r.objectKey,
	})
}

func (r *S3UploadedRequest) UnmarshalJSON(bs []byte) error {
	var aux struct {
		BucketName string `json:"bucket_name,omitempty"`
		ObjectKey  string `json:"object_key,omitempty"`
		*JSONSerializableRequest
	}
	if err := json.Unmarshal(bs, &aux); err != nil {
		return err
	}
	r.bucketName = aux.BucketName
	r.objectKey = aux.ObjectKey
	r.JSONSerializableRequest = aux.JSONSerializableRequest
	if !r.IsUploaded() {
		return nil
	}
	if r.HeadObjectClient == nil {
		return fmt.Errorf("head object client is not set")
	}
	if r.Downloader == nil {
		return fmt.Errorf("downloader is not set")
	}
	head, err := r.HeadObjectClient.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(r.objectKey),
	})
	if err != nil {
		return fmt.Errorf("failed to head object: %w", err)
	}
	buf := manager.NewWriteAtBuffer(make([]byte, head.ContentLength))
	_, err = r.Downloader.Download(context.Background(), buf, &s3.GetObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(r.objectKey),
	})
	if err != nil {
		return fmt.Errorf("failed to download request: %w", err)
	}
	objectBody := buf.Bytes()[:head.ContentLength]
	return json.Unmarshal(objectBody, &r.JSONSerializableRequest)
}
