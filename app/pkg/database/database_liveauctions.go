package database

import (
	"fmt"
	"time"

	"github.com/sotah-inc/server/app/pkg/sotah"

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

func liveAuctionsDatabasePath(dirPath string, rea sotah.Realm) string {
	return fmt.Sprintf("%s/%s/%s/live-auctions.db", dirPath, rea.Region.Name, rea.Slug)
}

func newLiveAuctionsDatabase(dirPath string, rea sotah.Realm) (liveAuctionsDatabase, error) {
	dbFilepath := liveAuctionsDatabasePath(dirPath, rea)
	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return liveAuctionsDatabase{}, err
	}

	return liveAuctionsDatabase{db, rea}, nil
}

type liveAuctionsDatabase struct {
	db    *bolt.DB
	realm sotah.Realm
}

func (ladBase liveAuctionsDatabase) persistMiniAuctionList(maList sotah.MiniAuctionList) error {
	logging.WithFields(logrus.Fields{
		"db":                ladBase.db.Path(),
		"miniauctions-list": len(maList),
	}).Debug("Persisting mini-auction-list")

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

func (ladBase liveAuctionsDatabase) GetMiniAuctionList() (sotah.MiniAuctionList, error) {
	out := sotah.MiniAuctionList{}

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
		out, err = sotah.NewMiniAuctionListFromGzipped(bkt.Get(liveAuctionsKeyName()))
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return sotah.MiniAuctionList{}, err
	}

	return out, nil
}

type miniAuctionListStats struct {
	TotalAuctions int
	OwnerNames    []sotah.OwnerName
	ItemIds       []blizzard.ItemID
	AuctionIds    []int64
}

func (ladBase liveAuctionsDatabase) stats() (miniAuctionListStats, error) {
	maList, err := ladBase.GetMiniAuctionList()
	if err != nil {
		return miniAuctionListStats{}, err
	}

	out := miniAuctionListStats{
		TotalAuctions: maList.TotalAuctions(),
		OwnerNames:    maList.OwnerNames(),
		ItemIds:       maList.ItemIds(),
		AuctionIds:    maList.AuctionIds(),
	}

	return out, nil
}

func NewLiveAuctionsDatabases(dirPath string, stas sotah.Statuses) (LiveAuctionsDatabases, error) {
	ladBases := LiveAuctionsDatabases{}

	for regionName, status := range stas {
		ladBases[regionName] = map[blizzard.RealmSlug]liveAuctionsDatabase{}

		for _, rea := range status.Realms {
			ladBase, err := newLiveAuctionsDatabase(dirPath, rea)
			if err != nil {
				return LiveAuctionsDatabases{}, err
			}

			ladBases[regionName][rea.Slug] = ladBase
		}
	}

	return ladBases, nil
}

type LiveAuctionsDatabases map[blizzard.RegionName]map[blizzard.RealmSlug]liveAuctionsDatabase

type liveAuctionsDatabasesLoadResult struct {
	Realm                sotah.Realm
	LastModified         time.Time
	Stats                miniAuctionListStats
	TotalRemovedAuctions int
	TotalNewAuctions     int
}

func (ladBases LiveAuctionsDatabases) Load(in chan LoadInJob) chan liveAuctionsDatabasesLoadResult {
	// establishing channels
	out := make(chan liveAuctionsDatabasesLoadResult)

	// spinning up the workers for fetching auctions
	worker := func() {
		for job := range in {
			// resolving the live-auctions database and gathering current Stats
			ladBase := ladBases[job.Realm.Region.Name][job.Realm.Slug]
			malStats, err := ladBase.stats()
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.Realm.Region.Name,
					"realm":  job.Realm.Slug,
				}).Error("Failed to gather live-auctions Stats")

				continue
			}

			// starting a load result
			result := liveAuctionsDatabasesLoadResult{
				Realm:        job.Realm,
				LastModified: job.TargetTime,
				Stats:        malStats,
			}

			// gathering previous and new auction ids for comparison
			removedAuctionIds := map[int64]struct{}{}
			for _, auc := range malStats.AuctionIds {
				removedAuctionIds[auc] = struct{}{}
			}
			newAuctionIds := map[int64]struct{}{}
			for _, auc := range job.Auctions {
				if _, ok := removedAuctionIds[auc.Auc]; ok {
					delete(removedAuctionIds, auc.Auc)
				}

				newAuctionIds[auc.Auc] = struct{}{}
			}
			for _, auc := range malStats.AuctionIds {
				if _, ok := newAuctionIds[auc]; ok {
					delete(newAuctionIds, auc)
				}
			}
			result.TotalNewAuctions = len(newAuctionIds)
			result.TotalRemovedAuctions = len(removedAuctionIds)

			maList := sotah.NewMiniAuctionListFromMiniAuctions(sotah.NewMiniAuctions(job.Auctions))
			if err := ladBase.persistMiniAuctionList(maList); err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.Realm.Region.Name,
					"realm":  job.Realm.Slug,
				}).Error("Failed to persist mini-auction-list")

				continue
			}

			out <- result
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	return out
}

type getAllStatsJob struct {
	Err   error
	Realm sotah.Realm
	Stats miniAuctionListStats
}

func (ladBases LiveAuctionsDatabases) GetStats(realms sotah.Realms) chan getAllStatsJob {
	in := make(chan sotah.Realm)
	out := make(chan getAllStatsJob)

	worker := func() {
		for rea := range in {
			stats, err := ladBases[rea.Region.Name][rea.Slug].stats()
			out <- getAllStatsJob{err, rea, stats}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	go func() {
		for _, rea := range realms {
			in <- rea
		}

		close(in)
	}()

	return out
}
