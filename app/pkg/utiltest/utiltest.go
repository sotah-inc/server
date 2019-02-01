package utiltest

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/stretchr/testify/assert"
)

// ReadFile - reads a file from a relative path
func ReadFile(relativePath string) ([]byte, error) {
	path, err := filepath.Abs(relativePath)
	if err != nil {
		return []byte{}, err
	}

	return ioutil.ReadFile(path)
}

// ServeFile - services a file up in an httptest server
func ServeFile(relativePath string) (*httptest.Server, error) {
	body, err := ReadFile(relativePath)
	if err != nil {
		return nil, err
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Plan-Qps-Allotted", "0")
		w.Header().Add("X-Plan-Qps-Current", "0")
		w.Header().Add("X-Plan-Quota-Allotted", "0")
		w.Header().Add("X-Plan-Quota-Current", "0")
		fmt.Fprintln(w, string(body))
	}))

	return ts, nil
}

func ValidateStatus(t *testing.T, reg sotah.Region, s sotah.Status) bool {
	if !assert.NotEmpty(t, s.Realms) {
		return false
	}

	for _, rea := range s.Realms {
		if !assert.Equal(t, reg.Hostname, rea.Region.Hostname) {
			return false
		}
	}

	return true
}
