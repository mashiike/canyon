package canyon

import (
	"encoding/json"
	"testing"

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
