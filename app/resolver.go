package main

import "net/url"

func newResolver() resolver {
	return resolver{
		getStatusURL:      defaultGetStatusURL,
		getAuctionInfoURL: defaultGetAuctionInfoURL,
		getAuctionsURL:    defaultGetAuctionsURL,
	}
}

func (r resolver) appendAPIKey(urlValue string) (string, error) {
	u, err := url.Parse(urlValue)
	if err != nil {
		return "", err
	}

	q := u.Query()
	q.Set("apikey", r.apiKey)
	u.RawQuery = q.Encode()

	return u.String(), nil
}

type resolver struct {
	apiKey string

	getStatusURL      getStatusURLFunc
	getAuctionInfoURL getAuctionInfoURLFunc
	getAuctionsURL    getAuctionsURLFunc
}
