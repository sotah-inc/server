package util

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"sync"
)

// Work - spawns N number of goroutines to execute X() in parallel, with Y() called when they exit
func Work(workerCount int, worker func(), postWork func()) {
	wg := &sync.WaitGroup{}
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			worker()
		}()
	}

	go func() {
		wg.Wait()
		postWork()
	}()
}

// Download - performs HTTP GET request against url, including adding gzip header and ungzipping
func Download(url string) (b []byte, err error) {
	var (
		req    *http.Request
		reader io.ReadCloser
	)

	// forming a request
	req, err = http.NewRequest("GET", url, nil)
	if err != nil {
		return b, err
	}
	req.Header.Add("Accept-Encoding", "gzip")

	// running it into a client
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return b, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return b, fmt.Errorf("Response was not OK: %d", resp.StatusCode)
	}

	// optionally decompressing it
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	return ioutil.ReadAll(reader)
}

// ReadFile - reads a file from a relative path
func ReadFile(relativePath string) ([]byte, error) {
	path, err := filepath.Abs(relativePath)
	if err != nil {
		return []byte{}, err
	}

	return ioutil.ReadFile(path)
}

// GzipEncode - gzip encodes a byte array
func GzipEncode(in []byte) ([]byte, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(in); err != nil {
		return nil, err
	}
	w.Close()

	return b.Bytes(), nil
}

// GzipDecode - gzip decodes a byte array
func GzipDecode(in []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}
