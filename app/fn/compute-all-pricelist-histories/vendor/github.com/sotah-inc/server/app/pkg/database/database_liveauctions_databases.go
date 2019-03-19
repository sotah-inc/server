package database

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
)

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

type LiveAuctionsLoadEncodedDataInJob struct {
	RegionName  blizzard.RegionName
	RealmSlug   blizzard.RealmSlug
	EncodedData []byte
}

type LiveAuctionsLoadEncodedDataOutJob struct {
	Err        error
	RegionName blizzard.RegionName
	RealmSlug  blizzard.RealmSlug
}

func (job LiveAuctionsLoadEncodedDataOutJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":  job.Err.Error(),
		"region": job.RegionName,
		"realm":  job.RealmSlug,
	}
}

func (ladBases LiveAuctionsDatabases) LoadEncodedData(in chan LiveAuctionsLoadEncodedDataInJob) chan LiveAuctionsLoadEncodedDataOutJob {
	// establishing channels
	out := make(chan LiveAuctionsLoadEncodedDataOutJob)

	// spinning up workers for receiving encoded-data and persisting it
	worker := func() {
		for job := range in {
			// resolving the live-auctions database and gathering current Stats
			ladBase := ladBases[job.RegionName][job.RealmSlug]

			if err := ladBase.persistEncodedData(job.EncodedData); err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.RegionName,
					"realm":  job.RealmSlug,
				}).Error("Failed to persist encoded-data")

				out <- LiveAuctionsLoadEncodedDataOutJob{
					Err:        err,
					RegionName: job.RegionName,
					RealmSlug:  job.RealmSlug,
				}

				continue
			}

			out <- LiveAuctionsLoadEncodedDataOutJob{
				Err:        nil,
				RegionName: job.RegionName,
				RealmSlug:  job.RealmSlug,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

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
