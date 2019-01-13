package blizzard

import (
	"io/ioutil"
	"net/http"

	"github.com/sotah-inc/server/app/metric"
	"github.com/sotah-inc/server/app/util"
)

// ResponseMeta is a blizzard api response meta data
type ResponseMeta struct {
	ContentLength int
	Body          []byte
	Status        int
}

// Download - performs HTTP GET request against url, including adding gzip header and ungzipping
func Download(url string) (ResponseMeta, error) {
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

	// parsing the body
	body, isGzipped, err := func() ([]byte, bool, error) {
		defer resp.Body.Close()

		isGzipped := resp.Header.Get("Content-Encoding") == "gzip"
		out, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return []byte{}, false, err
		}

		return out, isGzipped, nil
	}()
	if err != nil {
		return ResponseMeta{}, err
	}

	// logging network ingress
	contentLength := len(body)
	if err := metric.ReportBlizzardAPIIngress(url, contentLength); err != nil {
		return ResponseMeta{}, err
	}

	// optionally decoding the response body
	decodedBody, err := func() ([]byte, error) {
		if !isGzipped {
			return body, nil
		}

		return util.GzipDecode(body)
	}()
	if err != nil {
		return ResponseMeta{}, err
	}

	if resp.StatusCode != 200 {
		return ResponseMeta{Body: body, Status: resp.StatusCode}, nil
	}

	return ResponseMeta{
		ContentLength: contentLength,
		Body:          decodedBody,
		Status:        resp.StatusCode,
	}, nil
}
