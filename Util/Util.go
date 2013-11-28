package Util

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"time"
)

/*
	misc
*/
const writeLayout = "2006-01-02 03:04:05PM"

/*
	funcs
*/
func MemoryUsage() uint64 {
	s := &runtime.MemStats{}
	runtime.ReadMemStats(s)
	return s.Alloc
}

func Write(s string) {
	fmt.Println(fmt.Sprintf("[%s] %s", time.Now().Format(writeLayout), s))
}

func Conclude() {
	Write(fmt.Sprintf("Success! %.2f MB", float64(MemoryUsage())/1000/1000))
}

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
