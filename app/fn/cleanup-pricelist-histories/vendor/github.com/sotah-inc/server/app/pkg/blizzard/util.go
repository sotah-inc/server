package blizzard

import (
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/sotah-inc/server/app/pkg/util"
)

type timedTransport struct {
	rtp       http.RoundTripper
	dialer    *net.Dialer
	connStart time.Time
	connEnd   time.Time
	reqStart  time.Time
	reqEnd    time.Time
}

func newTimedTransport() *timedTransport {
	tr := &timedTransport{
		dialer: &net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		},
	}
	tr.rtp = &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		Dial:                tr.dial,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	return tr
}

func (tr *timedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	tr.reqStart = time.Now()
	resp, err := tr.rtp.RoundTrip(r)
	tr.reqEnd = time.Now()
	return resp, err
}

func (tr *timedTransport) dial(network, addr string) (net.Conn, error) {
	tr.connStart = time.Now()
	cn, err := tr.dialer.Dial(network, addr)
	tr.connEnd = time.Now()
	return cn, err
}

func (tr *timedTransport) ReqDuration() time.Duration {
	return tr.Duration() - tr.ConnDuration()
}

func (tr *timedTransport) ConnDuration() time.Duration {
	return tr.connEnd.Sub(tr.connStart)
}

func (tr *timedTransport) Duration() time.Duration {
	return tr.reqEnd.Sub(tr.reqStart)
}

type ResponseError struct {
	Body   string `json:"body"`
	Status int    `json:"status"`
	URI    string `json:"uri"`
}

// ResponseMeta is a blizzard api response meta data
type ResponseMeta struct {
	ContentLength      int
	Body               []byte
	Status             int
	ConnectionDuration time.Duration
	RequestDuration    time.Duration
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
	tp := newTimedTransport()
	httpClient := &http.Client{Transport: tp}
	resp, err := httpClient.Do(req)
	if err != nil {
		return ResponseMeta{}, err
	}
	defer resp.Body.Close()

	respMeta := ResponseMeta{
		ContentLength:      0,
		Body:               []byte{},
		Status:             resp.StatusCode,
		ConnectionDuration: tp.ConnDuration(),
		RequestDuration:    tp.ReqDuration(),
	}

	// parsing the body
	body, isGzipped, err := func() ([]byte, bool, error) {
		isGzipped := resp.Header.Get("Content-Encoding") == "gzip"
		out, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return []byte{}, false, err
		}

		return out, isGzipped, nil
	}()
	if err != nil {
		return respMeta, err
	}
	respMeta.ContentLength = len(body)

	// optionally decoding the response body
	decodedBody, err := func() ([]byte, error) {
		if !isGzipped {
			return body, nil
		}

		return util.GzipDecode(body)
	}()
	if err != nil {
		return respMeta, err
	}
	respMeta.Body = decodedBody

	return respMeta, nil
}
