package internal

import (
	"errors"
	"time"

	"github.com/sotah-inc/server/app/pkg/sotah"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/diskstore"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
)

type ResolverConfig struct {
	BlizzardClient blizzard.Client
	Messenger      messenger.Messenger
	Store          store.Store
	DiskStore      diskstore.DiskStore

	UseGCloud bool
}

func (rc ResolverConfig) toResolver() Resolver {
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

func NewResolver(rc ResolverConfig) Resolver { return rc.toResolver() }

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

func (r Resolver) GetAuctionsForRealm(rea sotah.Realm) (blizzard.Auctions, time.Time, error) {
	uri, err := r.AppendAccessToken(r.GetAuctionInfoURL(rea.Region.Hostname, rea.Slug))
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}

	// resolving auction-info from the api
	aInfo, _, err := blizzard.NewAuctionInfoFromHTTP(uri)
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}

	// validating the list of files
	if len(aInfo.Files) == 0 {
		return blizzard.Auctions{}, time.Time{}, errors.New("Cannot fetch Auctions with blank files")
	}
	aFile := aInfo.Files[0]

	// validating the Realm Region
	if rea.Region.Name == "" {
		return blizzard.Auctions{}, time.Time{}, errors.New("Region name cannot be blank")
	}

	// optionally downloading where the Realm has stale data
	if rea.LastModified == 0 || time.Unix(rea.LastModified, 0).Before(aFile.LastModifiedAsTime()) {
		aucs, err := r.downloadAndCacheAuctionData(rea, aFile)
		if err != nil {
			return blizzard.Auctions{}, time.Time{}, err
		}

		return aucs, aFile.LastModifiedAsTime(), nil
	}

	return blizzard.Auctions{}, time.Time{}, nil
}

func (r Resolver) downloadAndCacheAuctionData(rea sotah.Realm, aFile blizzard.AuctionFile) (blizzard.Auctions, error) {
	// downloading the auction data
	resp, err := blizzard.Download(aFile.URL)
	if err != nil {
		return blizzard.Auctions{}, err
	}

	// gathering the encoded body
	encodedBody, err := util.GzipEncode(resp.Body)

	if r.UseGCloud {
		logging.WithFields(logrus.Fields{
			"Region":       rea.Region.Name,
			"Realm":        rea.Slug,
			"LastModified": aFile.LastModifiedAsTime().Unix(),
			"encodedBody":  len(encodedBody),
		}).Debug("Writing auction data to gcloud Store")

		// writing the auction data to the gcloud storage
		if err := r.Store.WriteRealmAuctions(rea, aFile.LastModifiedAsTime(), encodedBody); err != nil {
			logging.WithFields(logrus.Fields{
				"error":        err.Error(),
				"Region":       rea.Region.Name,
				"Realm":        rea.Slug,
				"LastModified": aFile.LastModifiedAsTime().Unix(),
			}).Debug("Failed to write Auctions to gcloud storage")

			return blizzard.Auctions{}, err
		}

		return blizzard.NewAuctions(resp.Body)
	}

	// writing the auction data to the cache dir
	logging.WithFields(logrus.Fields{
		"Region":      rea.Region.Name,
		"Realm":       rea.Slug,
		"encodedBody": len(encodedBody),
	}).Debug("Writing auction data to cache dir")
	if err != nil {
		return blizzard.Auctions{}, err
	}
	if err := r.DiskStore.WriteAuctions(rea, encodedBody); err != nil {
		return blizzard.Auctions{}, err
	}

	return blizzard.NewAuctions(resp.Body)
}

type GetAuctionsJob struct {
	Err          error
	Realm        internal.Realm
	Auctions     blizzard.Auctions
	LastModified time.Time
}

func (r Resolver) GetAuctionsForRealms(reas internal.Realms) chan GetAuctionsJob {
	// establishing channels
	out := make(chan GetAuctionsJob)
	in := make(chan internal.Realm)

	// spinning up the workers for fetching Auctions
	worker := func() {
		for rea := range in {
			aucs, lastModified, err := r.GetAuctionsForRealm(rea)

			// optionally skipping draining out due to error
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"Region": rea.Region.Name,
					"Realm":  rea.Slug,
				}).Error("Auction fetch failure")

				continue
			}

			// optionally skipping draining out due to no new data
			if lastModified.IsZero() {
				logging.WithFields(logrus.Fields{
					"Region": rea.Region.Name,
					"Realm":  rea.Slug,
				}).Info("No Auctions received")

				continue
			}

			// draining out
			logging.WithFields(logrus.Fields{
				"Region":   rea.Region.Name,
				"Realm":    rea.Slug,
				"Auctions": len(aucs.Auctions),
			}).Debug("Auctions received")
			out <- GetAuctionsJob{nil, rea, aucs, lastModified}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the Realms
	go func() {
		for _, rea := range reas {
			logging.WithFields(logrus.Fields{
				"Region": rea.Region.Name,
				"Realm":  rea.Slug,
			}).Debug("Queueing up auction for downloading")
			in <- rea
		}

		close(in)
	}()

	return out
}
