package resolver

import (
	"errors"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
)

func (r Resolver) NewAuctionInfoFromHTTP(uri string) (blizzard.AuctionInfo, error) {
	resp, err := r.Download(uri, true)
	if err != nil {
		return blizzard.AuctionInfo{}, err
	}
	if resp.Status != http.StatusOK {
		return blizzard.AuctionInfo{}, errors.New("response status was not 200")
	}

	return blizzard.NewAuctionInfo(resp.Body)
}

func (r Resolver) NewAuctionsFromHTTP(uri string) (blizzard.Auctions, error) {
	resp, err := r.Download(uri, false)
	if err != nil {
		return blizzard.Auctions{}, err
	}
	if resp.Status != http.StatusOK {
		return blizzard.Auctions{}, errors.New("response status was not 200")
	}

	return blizzard.NewAuctions(resp.Body)
}

func (r Resolver) GetAuctionsForRealm(rea sotah.Realm) (blizzard.Auctions, time.Time, error) {
	// resolving auction-info from the api
	aInfo, err := r.NewAuctionInfoFromHTTP(r.GetAuctionInfoURL(rea.Region.Hostname, rea.Slug))
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}

	// validating the list of files
	if len(aInfo.Files) == 0 {
		return blizzard.Auctions{}, time.Time{}, errors.New("cannot fetch auctions with blank files")
	}
	aFile := aInfo.Files[0]

	// optionally downloading where the Realm has stale data
	if rea.LastModified == 0 || time.Unix(rea.LastModified, 0).Before(aFile.LastModifiedAsTime()) {
		aucs, err := r.NewAuctionsFromHTTP(aFile.URL)
		if err != nil {
			return blizzard.Auctions{}, time.Time{}, err
		}

		return aucs, aFile.LastModifiedAsTime(), nil
	}

	return blizzard.Auctions{}, time.Time{}, nil
}

type GetAuctionsJob struct {
	Err          error
	Realm        sotah.Realm
	Auctions     blizzard.Auctions
	LastModified time.Time
}

func (job GetAuctionsJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":         job.Err.Error(),
		"region":        job.Realm.Region.Name,
		"realm":         job.Realm.Slug,
		"last-modified": job.LastModified.Unix(),
	}
}

func (r Resolver) GetAuctionsForRealms(reas sotah.Realms) chan GetAuctionsJob {
	// establishing channels
	out := make(chan GetAuctionsJob)
	in := make(chan sotah.Realm)

	// spinning up the workers for fetching Auctions
	worker := func() {
		for rea := range in {
			aucs, lastModified, err := r.GetAuctionsForRealm(rea)

			// optionally halting on error
			if err != nil {
				out <- GetAuctionsJob{err, rea, blizzard.Auctions{}, lastModified}

				continue
			}

			// optionally skipping draining out due to no new data
			if lastModified.IsZero() {
				logging.WithFields(logrus.Fields{
					"region": rea.Region.Name,
					"realm":  rea.Slug,
				}).Info("No auctions received")

				continue
			}

			// draining out valid data received
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
