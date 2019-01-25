package diskstore

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
	"os"
	"time"
)

func (ds DiskStore) resolveAuctionsFilepath(rea sotah.Realm) (string, error) {
	if len(ds.CacheDir) == 0 {
		return "", errors.New("cache dir cannot be blank")
	}

	if len(rea.Region.Name) == 0 {
		return "", errors.New("region name cannot be blank")
	}

	if len(rea.Slug) == 0 {
		return "", errors.New("realm slug cannot be blank")
	}

	return fmt.Sprintf("%s/auctions/%s/%s.json.gz", ds.CacheDir, rea.Region.Name, rea.Slug), nil
}

func (ds DiskStore) WriteAuctions(rea sotah.Realm, data []byte) error {
	dest, err := ds.resolveAuctionsFilepath(rea)
	if err != nil {
		return err
	}

	return util.WriteFile(dest, data)
}

func (ds DiskStore) GetAuctionsByRealm(rea sotah.Realm) (blizzard.Auctions, time.Time, error) {
	// resolving the cached auctions filepath
	cachedAuctionsFilepath, err := ds.resolveAuctionsFilepath(rea)
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}

	// optionally skipping non-exist auctions files
	cachedAuctionsStat, err := os.Stat(cachedAuctionsFilepath)
	if err != nil {
		if !os.IsNotExist(err) {
			return blizzard.Auctions{}, time.Time{}, err
		}

		return blizzard.Auctions{}, time.Time{}, nil
	}

	// loading the gzipped cached Auctions file
	logging.WithFields(logrus.Fields{
		"region":   rea.Region.Name,
		"realm":    rea.Slug,
		"filepath": cachedAuctionsFilepath,
	}).Debug("Loading auctions from filepath")
	aucs, err := blizzard.NewAuctionsFromGzFilepath(cachedAuctionsFilepath)
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}
	logging.WithFields(logrus.Fields{
		"region":   rea.Region.Name,
		"realm":    rea.Slug,
		"filepath": cachedAuctionsFilepath,
	}).Debug("Finished loading auctions from filepath")

	return aucs, cachedAuctionsStat.ModTime(), nil
}

type GetAuctionsByRealmsJob struct {
	Err          error
	Realm        sotah.Realm
	Auctions     blizzard.Auctions
	LastModified time.Time
}

func (ds DiskStore) GetAuctionsByRealms(reas sotah.Realms) chan GetAuctionsByRealmsJob {
	// establishing channels
	out := make(chan GetAuctionsByRealmsJob)
	in := make(chan sotah.Realm)

	// spinning up the workers for fetching Auctions
	worker := func() {
		for rea := range in {
			aucs, lastModified, err := ds.GetAuctionsByRealm(rea)
			if lastModified.IsZero() {
				logging.WithFields(logrus.Fields{
					"Region": rea.Region.Name,
					"Realm":  rea.Slug,
				}).Error("Last-modified was blank when loading auctions from filecache")

				continue
			}

			out <- GetAuctionsByRealmsJob{err, rea, aucs, lastModified}
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
			}).Debug("Queueing up auction for loading")
			in <- rea
		}

		close(in)
	}()

	return out
}
