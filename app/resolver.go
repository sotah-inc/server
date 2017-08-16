package main

import (
	"fmt"
	"net/url"

	"github.com/ihsw/go-download/app/util"
)

func newResolver(c *config) resolver {
	return resolver{
		config: c,

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
	q.Set("apikey", r.config.APIKey)
	u.RawQuery = q.Encode()

	return u.String(), nil
}

func (r resolver) get(destination string) ([]byte, error) {
	if len(r.config.APIKey) > 0 {
		var err error
		destination, err = r.appendAPIKey(destination)
		if err != nil {
			return []byte{}, err
		}
	}

	fmt.Printf("url: %s\n", destination)
	return util.Download(destination)
}

type resolver struct {
	config *config

	getStatusURL      getStatusURLFunc
	getAuctionInfoURL getAuctionInfoURLFunc
	getAuctionsURL    getAuctionsURLFunc
}
