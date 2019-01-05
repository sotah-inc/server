package main

import (
	"github.com/sotah-inc/server/app/blizzard"
)

func newResolver(c config, mess messenger, stor store) resolver {
	return resolver{
		blizzardClient: blizzard.NewClient(c.ClientID, c.ClientSecret),
		config:         &c,
		messenger:      mess,
		store:          stor,

		getStatusURL:      blizzard.DefaultGetStatusURL,
		getAuctionInfoURL: blizzard.DefaultGetAuctionInfoURL,
		getAuctionsURL:    blizzard.DefaultGetAuctionsURL,
		getItemURL:        blizzard.DefaultGetItemURL,
		getItemIconURL:    defaultGetItemIconURL,
		getItemClassesURL: blizzard.DefaultGetItemClassesURL,
	}
}

type resolver struct {
	config         *config
	messenger      messenger
	store          store
	blizzardClient blizzard.Client

	getStatusURL      blizzard.GetStatusURLFunc
	getAuctionInfoURL blizzard.GetAuctionInfoURLFunc
	getAuctionsURL    blizzard.GetAuctionsURLFunc
	getItemURL        blizzard.GetItemURLFunc
	getItemIconURL    getItemIconURLFunc
	getItemClassesURL blizzard.GetItemClassesURLFunc
}

func (r resolver) appendAccessToken(destination string) (string, error) {
	return r.blizzardClient.AppendAccessToken(destination)
}
