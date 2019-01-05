package main

import (
	"net/url"

	"github.com/sotah-inc/server/app/blizzard"
)

func newResolver(c config, mess messenger, stor store) resolver {
	return resolver{
		config:    &c,
		messenger: mess,
		store:     stor,

		getStatusURL:      blizzard.DefaultGetStatusURL,
		getAuctionInfoURL: blizzard.DefaultGetAuctionInfoURL,
		getAuctionsURL:    blizzard.DefaultGetAuctionsURL,
		getItemURL:        blizzard.DefaultGetItemURL,
		getItemIconURL:    defaultGetItemIconURL,
		getItemClassesURL: blizzard.DefaultGetItemClassesURL,
	}
}

type resolver struct {
	config    *config
	messenger messenger
	store     store

	getStatusURL      blizzard.GetStatusURLFunc
	getAuctionInfoURL blizzard.GetAuctionInfoURLFunc
	getAuctionsURL    blizzard.GetAuctionsURLFunc
	getItemURL        blizzard.GetItemURLFunc
	getItemIconURL    getItemIconURLFunc
	getItemClassesURL blizzard.GetItemClassesURLFunc
}

func (r resolver) appendAccessToken(destination string) (string, error) {
	if r.config == nil {
		return destination, nil
	}

	apiKey := r.config.APIKey
	if apiKey == "" {
		return destination, nil
	}

	u, err := url.Parse(destination)
	if err != nil {
		return "", err
	}

	q := u.Query()
	q.Set("apikey", r.config.APIKey)
	u.RawQuery = q.Encode()

	return u.String(), nil
}
