package blizzard

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
)

// ResponseMeta is a blizzard api response meta data
type ResponseMeta struct {
	Body              []byte
	Status            int
	PlanQPSAllotted   int
	PlanQPSCurrent    int
	PlanQuotaAllotted int
	PlanQuotaCurrent  int
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

	// gathering api quota params
	planQPSAllotted, err := strconv.Atoi(resp.Header.Get("X-Plan-Qps-Allotted"))
	if err != nil {
		return ResponseMeta{}, err
	}
	planQPSCurrent, err := strconv.Atoi(resp.Header.Get("X-Plan-Qps-Current"))
	if err != nil {
		return ResponseMeta{}, err
	}
	planQuotaAllotted, err := strconv.Atoi(resp.Header.Get("X-Plan-Quota-Allotted"))
	if err != nil {
		return ResponseMeta{}, err
	}
	planQuotaCurrent, err := strconv.Atoi(resp.Header.Get("X-Plan-Quota-Current"))
	if err != nil {
		return ResponseMeta{}, err
	}

	return ResponseMeta{
		Body:              body,
		Status:            resp.StatusCode,
		PlanQPSAllotted:   planQPSAllotted,
		PlanQPSCurrent:    planQPSCurrent,
		PlanQuotaAllotted: planQuotaAllotted,
		PlanQuotaCurrent:  planQuotaCurrent,
	}, nil
}
