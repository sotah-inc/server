package main

import (
	"net/url"

	"github.com/ihsw/go-download/app/util"
)

func newResolver(apiKey string) resolver {
	return resolver{
		apiKey:            apiKey,
		getStatusURL:      defaultGetStatusURL,
		getAuctionInfoURL: defaultGetAuctionInfoURL,
		getAuctionsURL:    defaultGetAuctionsURL,
	}
}

func (r resolver) appendAPIKey(destination string) (string, error) {
	u, err := url.Parse(destination)
	if err != nil {
		return "", err
	}

	q := u.Query()
	q.Set("apikey", r.apiKey)
	u.RawQuery = q.Encode()

	return u.String(), nil
}

func (r resolver) get(destination string) ([]byte, error) {
	if len(r.apiKey) > 0 {
		var err error
		destination, err = r.appendAPIKey(destination)
		if err != nil {
			return []byte{}, err
		}
	}

	return util.Download(destination)
}

type resolver struct {
	apiKey string

	getStatusURL      getStatusURLFunc
	getAuctionInfoURL getAuctionInfoURLFunc
	getAuctionsURL    getAuctionsURLFunc
}
