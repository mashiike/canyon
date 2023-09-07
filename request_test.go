package canyon

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fujiwara/ridge"
	"github.com/stretchr/testify/require"
)

func TestNewJSONSerializableRequest(t *testing.T) {
	req, err := ridge.NewRequest(ReadFile(t, "testdata/http_event.json"))
	require.NoError(t, err, "should create request")
	serialized, err := NewJSONSerializableRequest(req)
	require.NoError(t, err, "should serialize request")
	bs, err := json.Marshal(serialized)
	require.NoError(t, err, "should marshal")
	require.JSONEq(t, string(ReadFile(t, "testdata/serialized_http_request.json")), string(bs), "same as expected serialized request")
}

func TestS3UploadedRequestUnmarshalJSON__NoS3Uploaded(t *testing.T) {
	var req S3UploadedRequest
	err := json.Unmarshal(ReadFile(t, "testdata/serialized_http_request.json"), &req)
	require.NoError(t, err, "should unmarshal")
	require.False(t, req.IsUploaded(), "should not be s3 uploaded")

	var expected JSONSerializableRequest
	err = json.Unmarshal(ReadFile(t, "testdata/serialized_http_request.json"), &expected)
	require.NoError(t, err, "should unmarshal")
	require.EqualValues(t, &expected, req.JSONSerializableRequest, "should be same as JSONSerializableRequest")
}

func TestS3UploadedRequestUnmarshalJSON__UploadedButNoSetClient(t *testing.T) {
	var req S3UploadedRequest
	err := json.Unmarshal(ReadFile(t, "testdata/s3_uploaded_request.json"), &req)
	require.EqualError(t, err, "head object client is not set")
}

func TestS3UploadedRequestUnmarsahlJSON__Uploaded(t *testing.T) {
	body := ReadFile(t, "testdata/serialized_http_request.json")
	s3Client := &mockS3Client{
		t: t,
		HeadObjectFunc: func(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			require.Equal(t, "my-bucket", *input.Bucket)
			require.Equal(t, "my-object", *input.Key)
			return &s3.HeadObjectOutput{
				ContentLength: int64(len(body)),
			}, nil
		},
		GetObjectFunc: func(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			require.Equal(t, "my-bucket", *input.Bucket)
			require.Equal(t, "my-object", *input.Key)
			return &s3.GetObjectOutput{
				Body:          io.NopCloser(bytes.NewReader(body)),
				ContentLength: int64(len(body)),
			}, nil
		},
	}
	var req S3UploadedRequest
	req.SetS3Client(s3Client)
	err := json.Unmarshal(ReadFile(t, "testdata/s3_uploaded_request.json"), &req)
	require.NoError(t, err, "should unmarshal")

	var expected JSONSerializableRequest
	err = json.Unmarshal(body, &expected)
	require.NoError(t, err, "should unmarshal")
	require.EqualValues(t, &expected, req.JSONSerializableRequest, "should be same as JSONSerializableRequest")
}

func TestS3UploadedRequestUpload__NotClientSet(t *testing.T) {
	var req S3UploadedRequest
	err := json.Unmarshal(ReadFile(t, "testdata/serialized_http_request.json"), &req)
	require.NoError(t, err, "should unmarshal")

	err = req.Upload(context.Background(), &url.URL{
		Scheme: "s3",
		Host:   "my-bucket",
		Path:   "/my-object",
	})
	require.EqualError(t, err, "uploader is not set")
}

func TestS3UploadedRequestUpload__Success(t *testing.T) {
	body := ReadFile(t, "testdata/serialized_http_request.json")
	s3Client := &mockS3Client{
		t: t,
		PutObjectFunc: func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			require.Equal(t, "my-bucket", *input.Bucket)
			require.Equal(t, "my-object", *input.Key)
			bs, err := io.ReadAll(input.Body)
			require.NoError(t, err, "should read body")
			require.JSONEq(t, string(body), string(bs))
			return &s3.PutObjectOutput{}, nil
		},
	}
	var req S3UploadedRequest
	req.SetS3Client(s3Client)
	err := json.Unmarshal(body, &req)
	require.NoError(t, err, "should unmarshal")

	err = req.Upload(context.Background(), &url.URL{
		Scheme: "s3",
		Host:   "my-bucket",
		Path:   "/my-object",
	})
	require.NoError(t, err, "should upload")
	require.True(t, req.IsUploaded(), "should be uploaded")

	actual, err := json.Marshal(&req)
	require.NoError(t, err, "should marshal")
	require.JSONEq(t, string(ReadFile(t, "testdata/s3_uploaded_request.json")), string(actual), "should be same as expected")
}
