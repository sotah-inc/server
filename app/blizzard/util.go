package blizzard

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/sotah-inc/server/app/metric"
)

// ResponseMeta is a blizzard api response meta data
type ResponseMeta struct {
	ContentLength int
	Body          []byte
	Status        int
}

// Download - performs HTTP GET request against url, including adding gzip header and ungzipping
func Download(url string) (ResponseMeta, error) {
	var (
		req    *http.Request
		reader io.ReadCloser
	)

	// forming a request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return ResponseMeta{}, err
	}
	req.Header.Add("Accept-Encoding", "gzip")

	// running it into a client
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return ResponseMeta{}, err
	}
	defer resp.Body.Close()

	// optionally parsing the body
	body := []byte{}
	if resp.StatusCode == http.StatusOK {
		// optionally decompressing it
		switch resp.Header.Get("Content-Encoding") {
		case "gzip":
			reader, err = gzip.NewReader(resp.Body)
			if err != nil {
				return ResponseMeta{}, err
			}
			defer reader.Close()
		default:
			reader = resp.Body
		}

		body, err = ioutil.ReadAll(reader)
		if err != nil {
			return ResponseMeta{}, err
		}
	}

	if resp.StatusCode != 200 {
		return ResponseMeta{Body: body, Status: resp.StatusCode}, nil
	}

	contentLength, err := strconv.Atoi(resp.Header.Get("Content-Length"))
	if err != nil {
		return ResponseMeta{}, err
	}

	metric.ReportBlizzardAPIIngress(url, contentLength)

	return ResponseMeta{
		ContentLength: contentLength,
		Body:          body,
		Status:        resp.StatusCode,
	}, nil
}
