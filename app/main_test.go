package app

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStatus(t *testing.T) {
	path, err := filepath.Abs("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	body, err := ioutil.ReadFile(path)
	if !assert.Nil(t, err) {
		return
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, string(body))
	}))

	s, err := getStatus(ts.URL)
	if !assert.NotEmpty(t, s.Realms) {
		return
	}
}
