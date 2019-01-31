package resolver

import (
	"github.com/sotah-inc/server/app/pkg/blizzard"
)

func NewResolver(bc blizzard.Client) Resolver {
	return Resolver{
		BlizzardClient:    bc,
		GetStatusURL:      blizzard.DefaultGetStatusURL,
		GetAuctionInfoURL: blizzard.DefaultGetAuctionInfoURL,
		GetAuctionsURL:    blizzard.DefaultGetAuctionsURL,
		GetItemURL:        blizzard.DefaultGetItemURL,
		GetItemIconURL:    blizzard.DefaultGetItemIconURL,
		GetItemClassesURL: blizzard.DefaultGetItemClassesURL,
	}
}

type Resolver struct {
	BlizzardClient    blizzard.Client
	GetStatusURL      blizzard.GetStatusURLFunc
	GetAuctionInfoURL blizzard.GetAuctionInfoURLFunc
	GetAuctionsURL    blizzard.GetAuctionsURLFunc
	GetItemURL        blizzard.GetItemURLFunc
	GetItemIconURL    blizzard.GetItemIconURLFunc
	GetItemClassesURL blizzard.GetItemClassesURLFunc
}

func (r Resolver) AppendAccessToken(destination string) (string, error) {
	return r.BlizzardClient.AppendAccessToken(destination)
}
