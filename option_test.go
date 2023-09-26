package canyon_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/mashiike/canyon"
	"github.com/mashiike/canyon/canyontest"
	"github.com/stretchr/testify/require"
)

func TestWithBackend(t *testing.T) {
	canyon.NewInMemoryBackend()
	b := canyon.NewInMemoryBackend()
	r := canyontest.NewRunner(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if canyon.IsWorker(r) {
				w.WriteHeader(http.StatusOK)
				return
			}
			canyon.SendToWorker(r, nil)
			w.WriteHeader(http.StatusOK)
		}),
		canyon.WithBackend(b),
	)
	defer r.Close()
	req, err := http.NewRequest(http.MethodPost, r.URL, strings.NewReader("test body"))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	actual := make([]string, 0)
	for _, v := range b.Entries() {
		actual = append(actual, string(v))
	}
	require.ElementsMatch(t, []string{"test body"}, actual)
}
