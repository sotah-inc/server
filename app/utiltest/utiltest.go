package utiltest

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path/filepath"
)

// ServeFile - services a file up in an httptest server
func ServeFile(relativePath string) (*httptest.Server, error) {
	path, err := filepath.Abs(relativePath)
	if err != nil {
		return &httptest.Server{}, err
	}

	body, err := ioutil.ReadFile(path)
	if err != nil {
		return &httptest.Server{}, err
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, string(body))
	}))

	return ts, nil
}
