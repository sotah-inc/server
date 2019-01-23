package database

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/sotah-inc/server/app/internal"

	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/util"
)

func liveAuctionsBucketName() []byte {
	return []byte("live-auctions")
}

func liveAuctionsKeyName() []byte {
	return []byte("live-auctions")
}

func liveAuctionsDatabasePath(c internal.Config, reg internal.Region, rea internal.Realm) (string, error) {
	return filepath.Abs(
		fmt.Sprintf("%s/databases/%s/%s/live-auctions.db", c.CacheDir, reg.Name, rea.Slug),
	)
}

func newLiveAuctionsDatabase(c internal.Config, reg internal.Region, rea internal.Realm) (liveAuctionsDatabase, error) {
	dbFilepath, err := liveAuctionsDatabasePath(c, reg, rea)
	if err != nil {
		return liveAuctionsDatabase{}, err
	}

	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return liveAuctionsDatabase{}, err
	}

	return liveAuctionsDatabase{db, rea}, nil
}

type liveAuctionsDatabase struct {
	db    *bolt.DB
	realm internal.Realm
}

func (ladBase liveAuctionsDatabase) persistMiniauctions(maList internal.MiniAuctionList) error {
	logging.WithFields(logrus.Fields{
		"db":                ladBase.db.Path(),
		"miniauctions-list": len(maList),
	}).Debug("Persisting miniauctions-list")

	encodedData, err := maList.EncodeForDatabase()
	if err != nil {
		return err
	}

	err = ladBase.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(liveAuctionsBucketName())
		if err != nil {
			return err
		}

		if err := bkt.Put(liveAuctionsKeyName(), encodedData); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (ladBase liveAuctionsDatabase) getMiniauctions() (internal.MiniAuctionList, error) {
	out := internal.MiniAuctionList{}
	err := ladBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(liveAuctionsBucketName())
		if bkt == nil {
			logging.WithFields(logrus.Fields{
				"db":          ladBase.db.Path(),
				"bucket-name": string(liveAuctionsBucketName()),
			}).Error("Live-auctions bucket not found")

			return nil
		}

		var err error
		out, err = internal.NewMiniAuctionsListFromGzipped(bkt.Get(liveAuctionsKeyName()))
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return internal.MiniAuctionList{}, err
	}

	return out, nil
}

type miniAuctionListStats struct {
	totalAuctions int
	ownerNames    []internal.OwnerName
	itemIds       []blizzard.ItemID
	auctionIds    []int64
}

func (ladBase liveAuctionsDatabase) stats() (miniAuctionListStats, error) {
	maList, err := ladBase.getMiniauctions()
	if err != nil {
		return miniAuctionListStats{}, err
	}

	out := miniAuctionListStats{
		totalAuctions: maList.TotalAuctions(),
		ownerNames:    maList.OwnerNames(),
		itemIds:       maList.ItemIds(),
		auctionIds:    maList.AuctionIds(),
	}

	return out, nil
}

func newLiveAuctionsDatabases(c config, regs regionList, stas statuses) (LiveAuctionsDatabases, error) {
	ladBases := LiveAuctionsDatabases{}

	for _, reg := range regs {
		ladBases[reg.Name] = map[blizzard.RealmSlug]liveAuctionsDatabase{}

		for _, rea := range stas[reg.Name].Realms {
			ladBase, err := newLiveAuctionsDatabase(c, reg, rea)
			if err != nil {
				return LiveAuctionsDatabases{}, err
			}

			ladBases[reg.Name][rea.Slug] = ladBase
		}
	}

	return ladBases, nil
}

type LiveAuctionsDatabases map[regionName]map[blizzard.RealmSlug]liveAuctionsDatabase

type liveAuctionsDatabasesLoadResult struct {
	realm                realm
	lastModified         time.Time
	stats                miniAuctionListStats
	totalRemovedAuctions int
	totalNewAuctions     int
}

func (ladBases LiveAuctionsDatabases) load(in chan loadAuctionsJob) chan liveAuctionsDatabasesLoadResult {
	// establishing channels
	out := make(chan liveAuctionsDatabasesLoadResult)

	// spinning up the workers for fetching auctions
	worker := func() {
		for job := range in {
			// validating the job intake
			if job.err != nil {
				logging.WithFields(logrus.Fields{
					"error":  job.err.Error(),
					"region": job.realm.region.Name,
					"realm":  job.realm.Slug,
				}).Error("Failed to load auctions")

				continue
			}

			// resolving the live-auctions database and gathering current stats
			ladBase := ladBases[job.realm.region.Name][job.realm.Slug]
			malStats, err := ladBase.stats()
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.realm.region.Name,
					"realm":  job.realm.Slug,
				}).Error("Failed to gather live-auctions stats")

				continue
			}

			// starting a load result
			result := liveAuctionsDatabasesLoadResult{
				realm:        job.realm,
				lastModified: job.lastModified,
				stats:        malStats,
			}

			// gathering previous and new auction ids for comparison
			removedAuctionIds := map[int64]struct{}{}
			for _, auc := range malStats.auctionIds {
				removedAuctionIds[auc] = struct{}{}
			}
			newAuctionIds := map[int64]struct{}{}
			for _, auc := range job.auctions.Auctions {
				if _, ok := removedAuctionIds[auc.Auc]; ok {
					delete(removedAuctionIds, auc.Auc)
				}

				newAuctionIds[auc.Auc] = struct{}{}
			}
			for _, auc := range malStats.auctionIds {
				if _, ok := newAuctionIds[auc]; ok {
					delete(newAuctionIds, auc)
				}
			}
			result.totalNewAuctions = len(newAuctionIds)
			result.totalRemovedAuctions = len(removedAuctionIds)

			maList := newMiniAuctionListFromBlizzardAuctions(job.auctions.Auctions)
			if err := ladBase.persistMiniauctions(maList); err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.realm.region.Name,
					"realm":  job.realm.Slug,
				}).Error("Failed to persist mini-auctions")

				continue
			}

			out <- result
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(2, worker, postWork)

	return out
}

type getAllStatsJob struct {
	err   error
	realm realm
	stats miniAuctionListStats
}

func (ladBases LiveAuctionsDatabases) getStats(wList regionRealmMap) chan getAllStatsJob {
	in := make(chan liveAuctionsDatabase)
	out := make(chan getAllStatsJob)

	worker := func() {
		for ladBase := range in {
			stats, err := ladBase.stats()
			out <- getAllStatsJob{err, ladBase.realm, stats}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(2, worker, postWork)

	go func() {
		for rName, realmLiveAuctionDatabases := range ladBases {
			realmWhitelist, ok := func() (realmMap, bool) {
				if wList == nil {
					return realmMap{}, true
				}

				out, ok := wList[rName]
				if !ok {
					return realmMap{}, false
				}

				return out, true
			}()
			if !ok {
				continue
			}

			for rSlug, ladBase := range realmLiveAuctionDatabases {
				if wList != nil {
					if _, ok := realmWhitelist.values[rSlug]; !ok {
						continue
					}
				}

				in <- ladBase
			}
		}

		close(in)
	}()

	return out
}
