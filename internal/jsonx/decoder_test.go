package jsonx_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/mashiike/canyon/internal/jsonx"
	"github.com/stretchr/testify/require"
)

func TestDecoderSkipUntilValidToken(t *testing.T) {
	cases := []struct {
		casename string
		input    string
		expected string
	}{
		{
			casename: "skip invalid token",
			input:    `foo{"foo": "bar"}`,
			expected: `{"foo": "bar"}`,
		},
		{
			casename: "double invalid token",
			input:    `foo{{"foo": "bar"}`,
			expected: ``,
		},
		{
			casename: "new line invalid token",
			input:    `{\n{"foo": "bar"}`,
			expected: `{"foo": "bar"}`,
		},
		{
			casename: "hogehoge",
			input:    `hogehoge {"foo": "bar"}`,
			expected: `{"foo": "bar"}`,
		},
		{
			casename: "buffer empty",
			input:    `hogehoge {`,
			expected: ``,
		},
		{
			casename: "buffer eof",
			input:    `hogehoge`,
			expected: ``,
		},
	}

	for _, c := range cases {
		t.Run(c.casename, func(t *testing.T) {
			d := jsonx.NewDecoder(strings.NewReader(c.input))
			var raw json.RawMessage
			err := d.Decode(&raw)
			require.Error(t, err, "should return error, first decode")
			err = d.SkipUntilValidToken()
			require.NoError(t, err, "should return error")
			d.Decode(&raw)
			require.Equal(t, c.expected, string(raw), "expected next token")
		})
	}
}
