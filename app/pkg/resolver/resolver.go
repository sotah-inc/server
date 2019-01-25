package resolver

import (
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/diskstore"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/store"
)

type Config struct {
	BlizzardClient blizzard.Client
	Messenger      messenger.Messenger
	Store          store.Store
	DiskStore      diskstore.DiskStore

	UseGCloud bool
}

func (rc Config) toResolver() Resolver {
	return Resolver{
		BlizzardClient: rc.BlizzardClient,
		Messenger:      rc.Messenger,
		Store:          rc.Store,
		DiskStore:      rc.DiskStore,
		UseGCloud:      rc.UseGCloud,

		GetStatusURL:      blizzard.DefaultGetStatusURL,
		GetAuctionInfoURL: blizzard.DefaultGetAuctionInfoURL,
		GetAuctionsURL:    blizzard.DefaultGetAuctionsURL,
		GetItemURL:        blizzard.DefaultGetItemURL,
		GetItemIconURL:    blizzard.DefaultGetItemIconURL,
		GetItemClassesURL: blizzard.DefaultGetItemClassesURL,
	}
}

func NewResolver(rc Config) Resolver { return rc.toResolver() }

type Resolver struct {
	Messenger      messenger.Messenger
	Store          store.Store
	BlizzardClient blizzard.Client
	DiskStore      diskstore.DiskStore

	UseGCloud bool

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
