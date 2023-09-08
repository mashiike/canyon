package canyon

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func ReadFile(t *testing.T, path string) []byte {
	bs, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bs
}

func ReadJSON[T any](t *testing.T, path string) *T {
	var v T
	err := json.Unmarshal(ReadFile(t, path), &v)
	if err != nil {
		t.Fatal(err)
	}
	return &v
}

func TestCamelCaseToKebabCase(t *testing.T) {
	require.Equal(t, "Foo-Bar-Baz", camelCaseToKebabCase("FooBarBaz"))
	require.Equal(t, "foo-Bar-Baz", camelCaseToKebabCase("fooBarBaz"))
	require.Equal(t, "foo-bar-baz", camelCaseToKebabCase("foo-bar-baz"))
	require.Equal(t, "foo_bar_baz", camelCaseToKebabCase("foo_bar_baz"))
	require.Equal(t, "foo bar baz", camelCaseToKebabCase("foo bar baz"))
}

func TestKebabCaseToCamelCase(t *testing.T) {
	require.Equal(t, "FooBarBaz", kebabCaseToCamelCase("Foo-Bar-Baz"))
	require.Equal(t, "fooBarBaz", kebabCaseToCamelCase("foo-Bar-Baz"))
	require.Equal(t, "fooBarBaz", kebabCaseToCamelCase("foo-bar-baz"))
	require.Equal(t, "foo_bar_baz", kebabCaseToCamelCase("foo_bar_baz"))
	require.Equal(t, "foo bar baz", kebabCaseToCamelCase("foo bar baz"))
}

func TestRundomBytes(t *testing.T) {
	bs := randomBytes(32)
	require.Equal(t, 32, len(bs), "should have 32 bytes")
}

func TestGetExtension(t *testing.T) {
	cases := [][]string{
		{"application/json", ".json"},
		{"application/json; charset=utf-8", ".json"},
		{"application/json;charset=utf-8", ".json"},
		{"application/json; charset=utf-8; foo=bar", ".json"},
		{"image/png", ".png"},
		{"image/gif", ".gif"},
		{"image/jpeg", ".jpeg"},
		{"image/svg+xml", ".svg"},
		{"image/webp", ".webp"},
		{"text/html", ".html"},
		{"text/css", ".css"},
		{"text/javascript", ".js"},
		{"text/plain", ".txt"},
		{"", ".bin"},
	}
	for _, c := range cases {
		require.Equal(t, c[1], getExtension(c[0]), fmt.Sprintf("should get extension `%s`->`%s`", c[0], c[1]))
	}
}
