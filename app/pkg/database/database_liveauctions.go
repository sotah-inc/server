package database

import (
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
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
		"db":                 ladBase.db.Path(),
		"mini-auctions-list": len(maList),
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

type liveAuctionsLoadOutJob struct {
	Err                  error
	Realm                sotah.Realm
	LastModified         time.Time
	Stats                miniAuctionListStats
	TotalRemovedAuctions int
	TotalNewAuctions     int
}

func (job liveAuctionsLoadOutJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":         job.Err.Error(),
		"region":        job.Realm.Region.Name,
		"realm":         job.Realm.Slug,
		"last-modified": job.LastModified.Unix(),
	}
}

func (ladBases LiveAuctionsDatabases) Load(in chan LoadInJob) chan liveAuctionsLoadOutJob {
	// establishing channels
	out := make(chan liveAuctionsLoadOutJob)

	// spinning up workers for receiving auctions and persisting them
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
				}).Error("Failed to gather live-auctions stats")

				out <- liveAuctionsLoadOutJob{
					Err:                  err,
					Realm:                job.Realm,
					LastModified:         job.TargetTime,
					Stats:                miniAuctionListStats{},
					TotalRemovedAuctions: 0,
					TotalNewAuctions:     0,
				}

				continue
			}

			// gathering previous and new auction counts for metrics collection
			totalNewAuctions, totalRemovedAuctions := func() (int, int) {
				removedAuctionIds := map[int64]struct{}{}
				for _, auc := range malStats.AuctionIds {
					removedAuctionIds[auc] = struct{}{}
				}
				newAuctionIds := map[int64]struct{}{}
				for _, auc := range job.Auctions.Auctions {
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

				return len(newAuctionIds), len(removedAuctionIds)
			}()

			maList := sotah.NewMiniAuctionListFromMiniAuctions(sotah.NewMiniAuctions(job.Auctions))
			if err := ladBase.persistMiniAuctionList(maList); err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.Realm.Region.Name,
					"realm":  job.Realm.Slug,
				}).Error("Failed to persist mini-auction-list")

				out <- liveAuctionsLoadOutJob{
					Err:                  err,
					Realm:                job.Realm,
					LastModified:         job.TargetTime,
					Stats:                miniAuctionListStats{},
					TotalRemovedAuctions: 0,
					TotalNewAuctions:     0,
				}

				continue
			}

			out <- liveAuctionsLoadOutJob{
				Err:                  nil,
				Realm:                job.Realm,
				LastModified:         job.TargetTime,
				TotalNewAuctions:     totalNewAuctions,
				TotalRemovedAuctions: totalRemovedAuctions,
				Stats:                malStats,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(2, worker, postWork)

	return out
}

type GetStatsJob struct {
	Err   error
	Realm sotah.Realm
	Stats miniAuctionListStats
}

func (job GetStatsJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":  job.Err.Error(),
		"region": job.Realm.Region.Name,
		"realm":  job.Realm.Slug,
	}
}

func (ladBases LiveAuctionsDatabases) GetStats(realms sotah.Realms) chan GetStatsJob {
	in := make(chan sotah.Realm)
	out := make(chan GetStatsJob)

	worker := func() {
		for rea := range in {
			stats, err := ladBases[rea.Region.Name][rea.Slug].stats()
			out <- GetStatsJob{err, rea, stats}
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
