package blizzard

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/metric"
)

// OAuthTokenEndpoint - http endpoint for gathering new oauth access tokens
const OAuthTokenEndpoint = "https://us.battle.net/oauth/token?grant_type=client_credentials"

// NewClient - generates a client used for querying blizz api
func NewClient(id string, secret string) Client {
	return Client{id, secret, ""}
}

// Client - used for querying blizz api
type Client struct {
	id          string
	secret      string
	accessToken string
}

type refreshResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

// RefreshFromHTTP - gathers an access token from the oauth token endpoint
func (c Client) RefreshFromHTTP(uri string) (Client, error) {
	// forming a request
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return Client{}, err
	}

	// appending auth headers
	req.SetBasicAuth(c.id, c.secret)
	req.Header.Add("Accept-Encoding", "gzip")

	// producing an http client and running the request
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return Client{}, err
	}

	if resp.StatusCode != http.StatusOK {
		logrus.WithFields(logrus.Fields{
			"uri":    uri,
			"id":     c.id,
			"secret": c.secret,
		}).Info("Received failed oauth token response from Blizzard API")

		return Client{}, errors.New("OAuth token response was not 200")
	}

	contentLength, err := strconv.Atoi(resp.Header.Get("Content-Length"))
	if err != nil {
		return Client{}, err
	}

	metric.ReportBlizzardAPIIngress(uri, contentLength)

	// parsing the body
	body, err := func() ([]byte, error) {
		defer resp.Body.Close()

		switch resp.Header.Get("Content-Encoding") {
		case "gzip":
			reader, err := gzip.NewReader(resp.Body)
			if err != nil {
				return nil, err
			}
			defer reader.Close()

			return ioutil.ReadAll(reader)
		default:
			return ioutil.ReadAll(resp.Body)
		}
	}()
	if err != nil {
		return Client{}, err
	}

	// decoding the body
	r := &refreshResponse{}
	if err := json.Unmarshal(body, &r); err != nil {
		return Client{}, err
	}

	c.accessToken = r.AccessToken

	return c, nil
}

// AppendAccessToken - appends access token used for making authenticated requests
func (c Client) AppendAccessToken(destination string) (string, error) {
	if c.accessToken == "" {
		return "", errors.New("Could not append access token, access token is blank")
	}

	u, err := url.Parse(destination)
	if err != nil {
		return "", err
	}

	q := u.Query()
	q.Set("access_token", c.accessToken)
	u.RawQuery = q.Encode()

	return u.String(), nil
}
