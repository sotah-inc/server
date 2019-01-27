package resolver

import (
	"errors"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
)

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
		return blizzard.Auctions{}, time.Time{}, errors.New("cannot fetch Auctions with blank files")
	}
	aFile := aInfo.Files[0]

	// optionally downloading where the Realm has stale data
	if rea.LastModified == 0 || time.Unix(rea.LastModified, 0).Before(aFile.LastModifiedAsTime()) {
		resp, err := blizzard.Download(aFile.URL)
		if err != nil {
			return blizzard.Auctions{}, time.Time{}, err
		}

		aucs, err := blizzard.NewAuctions(resp.Body)
		if err != nil {
			return blizzard.Auctions{}, time.Time{}, err
		}

		return aucs.Auctions, aFile.LastModifiedAsTime(), nil
	}

	return blizzard.Auctions{}, time.Time{}, nil
}

type GetAuctionsJob struct {
	Err          error
	Realm        sotah.Realm
	Auctions     blizzard.Auctions
	LastModified time.Time
}

func (r Resolver) GetAuctionsForRealms(reas sotah.Realms) chan GetAuctionsJob {
	// establishing channels
	out := make(chan GetAuctionsJob)
	in := make(chan sotah.Realm)

	// spinning up the workers for fetching Auctions
	worker := func() {
		for rea := range in {
			aucs, lastModified, err := r.GetAuctionsForRealm(rea)

			// optionally skipping draining out due to error
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": rea.Region.Name,
					"realm":  rea.Slug,
				}).Error("Auction fetch failure")

				continue
			}

			// optionally skipping draining out due to no new data
			if lastModified.IsZero() {
				logging.WithFields(logrus.Fields{
					"region": rea.Region.Name,
					"realm":  rea.Slug,
				}).Info("No Auctions received")

				continue
			}

			// draining out
			logging.WithFields(logrus.Fields{
				"region":   rea.Region.Name,
				"realm":    rea.Slug,
				"auctions": len(aucs.Auctions),
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
				"region": rea.Region.Name,
				"realm":  rea.Slug,
			}).Debug("Queueing up auction for downloading")

			in <- rea
		}

		close(in)
	}()

	return out
}
