package main

import (
	"net/url"

	"github.com/ihsw/sotah-server/app/blizzard"
)

func newResolver(c config, mess messenger, stor storage) resolver {
	return resolver{
		config:    &c,
		messenger: mess,
		storage:   stor,

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
	storage   storage

	getStatusURL      blizzard.GetStatusURLFunc
	getAuctionInfoURL blizzard.GetAuctionInfoURLFunc
	getAuctionsURL    blizzard.GetAuctionsURLFunc
	getItemURL        blizzard.GetItemURLFunc
	getItemIconURL    getItemIconURLFunc
	getItemClassesURL blizzard.GetItemClassesURLFunc
}

func (r resolver) appendAPIKey(destination string) (string, error) {
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
