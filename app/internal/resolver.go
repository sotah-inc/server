package internal

import (
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/store"
)

func NewResolver(c Config, mess messenger.Messenger, stor store.Store) Resolver {
	return Resolver{
		blizzardClient: blizzard.NewClient(c.ClientID, c.ClientSecret),
		Config:         &c,
		Messenger:      mess,
		Store:          stor,

		getStatusURL:      blizzard.DefaultGetStatusURL,
		getAuctionInfoURL: blizzard.DefaultGetAuctionInfoURL,
		getAuctionsURL:    blizzard.DefaultGetAuctionsURL,
		getItemURL:        blizzard.DefaultGetItemURL,
		getItemIconURL:    defaultGetItemIconURL,
		getItemClassesURL: blizzard.DefaultGetItemClassesURL,
	}
}

type Resolver struct {
	Config         *Config
	Messenger      messenger.Messenger
	Store          store.Store
	blizzardClient blizzard.Client

	getStatusURL      blizzard.GetStatusURLFunc
	getAuctionInfoURL blizzard.GetAuctionInfoURLFunc
	getAuctionsURL    blizzard.GetAuctionsURLFunc
	getItemURL        blizzard.GetItemURLFunc
	getItemIconURL    getItemIconURLFunc
	getItemClassesURL blizzard.GetItemClassesURLFunc
}

func (r Resolver) appendAccessToken(destination string) (string, error) {
	return r.blizzardClient.AppendAccessToken(destination)
}
