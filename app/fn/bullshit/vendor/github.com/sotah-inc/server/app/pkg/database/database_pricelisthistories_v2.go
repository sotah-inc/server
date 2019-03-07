package database

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
)

// db
func pricelistHistoryDatabaseV2FilePath(
	dirPath string,
	regionName blizzard.RegionName,
	realmSlug blizzard.RealmSlug,
	normalizedTargetTimestamp sotah.UnixTimestamp,
) string {
	return fmt.Sprintf(
		"%s/%s/%s/pricelist-histories-v2-%d.db",
		dirPath,
		regionName,
		realmSlug,
		normalizedTargetTimestamp,
	)
}

func NewPricelistHistoryDatabasesV2(dirPath string, statuses sotah.Statuses) (PricelistHistoryDatabasesV2, error) {
	if len(dirPath) == 0 {
		return PricelistHistoryDatabasesV2{}, errors.New("dir-path cannot be blank")
	}

	phdBases := PricelistHistoryDatabasesV2{
		databaseDir: dirPath,
		Databases:   regionRealmDatabaseShardsV2{},
	}

	for regionName, regionStatuses := range statuses {
		phdBases.Databases[regionName] = realmDatabaseShardsV2{}

		for _, rea := range regionStatuses.Realms {
			phdBases.Databases[regionName][rea.Slug] = PricelistHistoryDatabaseV2Shards{}

			dbPathPairs, err := V2Paths(fmt.Sprintf("%s/%s/%s", dirPath, regionName, rea.Slug))
			if err != nil {
				return PricelistHistoryDatabasesV2{}, err
			}

			for _, dbPathPair := range dbPathPairs {
				phdBase, err := newPricelistHistoryDatabaseV2(dbPathPair.FullPath, dbPathPair.TargetTime)
				if err != nil {
					return PricelistHistoryDatabasesV2{}, err
				}

				phdBases.Databases[regionName][rea.Slug][sotah.UnixTimestamp(dbPathPair.TargetTime.Unix())] = phdBase
			}
		}
	}

	return phdBases, nil
}

type regionRealmDatabaseShardsV2 map[blizzard.RegionName]realmDatabaseShardsV2

type realmDatabaseShardsV2 map[blizzard.RealmSlug]PricelistHistoryDatabaseV2Shards

type PricelistHistoryDatabasesV2 struct {
	databaseDir string
	Databases   regionRealmDatabaseShardsV2
}

func (phdBases PricelistHistoryDatabasesV2) resolveDatabaseFromLoadInJob(job PricelistHistoryDatabaseV2LoadInJob) (PricelistHistoryDatabaseV2, error) {
	phdBase, ok := phdBases.Databases[job.RegionName][job.RealmSlug][job.NormalizedTargetTimestamp]
	if ok {
		return phdBase, nil
	}

	normalizedTargetDate := time.Unix(int64(job.NormalizedTargetTimestamp), 0)

	dbPath := pricelistHistoryDatabaseV2FilePath(
		phdBases.databaseDir,
		job.RegionName,
		job.RealmSlug,
		job.NormalizedTargetTimestamp,
	)
	phdBase, err := newPricelistHistoryDatabaseV2(dbPath, normalizedTargetDate)
	if err != nil {
		return PricelistHistoryDatabaseV2{}, err
	}
	phdBases.Databases[job.RegionName][job.RealmSlug][job.NormalizedTargetTimestamp] = phdBase

	return phdBase, nil
}

type PricelistHistoryDatabaseV2LoadInJob struct {
	RegionName                blizzard.RegionName
	RealmSlug                 blizzard.RealmSlug
	NormalizedTargetTimestamp sotah.UnixTimestamp
	Data                      map[blizzard.ItemID][]byte
}

type PricelistHistoryDatabaseV2LoadOutJob struct {
	Err                       error
	RegionName                blizzard.RegionName
	RealmSlug                 blizzard.RealmSlug
	NormalizedTargetTimestamp sotah.UnixTimestamp
}

func (job PricelistHistoryDatabaseV2LoadOutJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":                       job.Err.Error(),
		"region":                      job.RegionName,
		"realm":                       job.RealmSlug,
		"normalized-target-timestamp": job.NormalizedTargetTimestamp,
	}
}

func (phdBases PricelistHistoryDatabasesV2) Load(in chan PricelistHistoryDatabaseV2LoadInJob) chan PricelistHistoryDatabaseV2LoadOutJob {
	// establishing channels
	out := make(chan PricelistHistoryDatabaseV2LoadOutJob)

	// spinning up workers for receiving auctions and persisting them
	worker := func() {
		for job := range in {
			phdBase, err := phdBases.resolveDatabaseFromLoadInJob(job)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.RegionName,
					"realm":  job.RealmSlug,
				}).Error("Could not resolve database from load job")

				out <- PricelistHistoryDatabaseV2LoadOutJob{
					Err:                       err,
					RegionName:                job.RegionName,
					RealmSlug:                 job.RealmSlug,
					NormalizedTargetTimestamp: job.NormalizedTargetTimestamp,
				}

				continue
			}

			if err := phdBase.persistItemPrices(job.Data); err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.RegionName,
					"realm":  job.RealmSlug,
				}).Error("Could not persist item-prices from job")

				out <- PricelistHistoryDatabaseV2LoadOutJob{
					Err:                       err,
					RegionName:                job.RegionName,
					RealmSlug:                 job.RealmSlug,
					NormalizedTargetTimestamp: job.NormalizedTargetTimestamp,
				}

				continue
			}

			out <- PricelistHistoryDatabaseV2LoadOutJob{
				Err:                       nil,
				RegionName:                job.RegionName,
				RealmSlug:                 job.RealmSlug,
				NormalizedTargetTimestamp: job.NormalizedTargetTimestamp,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	return out
}

func (phdBases PricelistHistoryDatabasesV2) pruneDatabases() error {
	earliestUnixTimestamp := RetentionLimit().Unix()
	logging.WithField("limit", earliestUnixTimestamp).Info("Checking for databases to prune")
	for rName, realmDatabases := range phdBases.Databases {
		for rSlug, databaseShards := range realmDatabases {
			for unixTimestamp, phdBase := range databaseShards {
				if int64(unixTimestamp) > earliestUnixTimestamp {
					continue
				}

				logging.WithFields(logrus.Fields{
					"region":             rName,
					"realm":              rSlug,
					"database-timestamp": unixTimestamp,
				}).Debug("Removing database from shard map")
				delete(phdBases.Databases[rName][rSlug], unixTimestamp)

				dbPath := phdBase.db.Path()

				logging.WithFields(logrus.Fields{
					"region":             rName,
					"realm":              rSlug,
					"database-timestamp": unixTimestamp,
				}).Debug("Closing database")
				if err := phdBase.db.Close(); err != nil {
					logging.WithFields(logrus.Fields{
						"region":   rName,
						"realm":    rSlug,
						"database": dbPath,
					}).Error("Failed to close database")

					return err
				}

				logging.WithFields(logrus.Fields{
					"region":   rName,
					"realm":    rSlug,
					"filepath": dbPath,
				}).Debug("Deleting database file")
				if err := os.Remove(dbPath); err != nil {
					logging.WithFields(logrus.Fields{
						"region":   rName,
						"realm":    rSlug,
						"database": dbPath,
					}).Error("Failed to remove database file")

					return err
				}
			}
		}
	}

	return nil
}

func (phdBases PricelistHistoryDatabasesV2) StartPruner(stopChan sotah.WorkerStopChan) sotah.WorkerStopChan {
	onStop := make(sotah.WorkerStopChan)
	go func() {
		ticker := time.NewTicker(20 * time.Minute)

		logging.Info("Starting pruner")
	outer:
		for {
			select {
			case <-ticker.C:
				if err := phdBases.pruneDatabases(); err != nil {
					logging.WithField("error", err.Error()).Error("Failed to prune databases")

					continue
				}
			case <-stopChan:
				ticker.Stop()

				break outer
			}
		}

		onStop <- struct{}{}
	}()

	return onStop
}

type PricelistHistoryDatabaseV2Shards map[sotah.UnixTimestamp]PricelistHistoryDatabaseV2

func (phdShards PricelistHistoryDatabaseV2Shards) GetPriceHistory(
	rea sotah.Realm,
	ItemId blizzard.ItemID,
	lowerBounds time.Time,
	upperBounds time.Time,
) (sotah.PriceHistory, error) {
	pHistory := sotah.PriceHistory{}

	for _, phdBase := range phdShards {
		receivedHistory, err := phdBase.getItemPriceHistory(ItemId)
		if err != nil {
			return sotah.PriceHistory{}, err
		}

		for targetTimestamp, pricesValue := range receivedHistory {
			if int64(targetTimestamp) < lowerBounds.Unix() {
				continue
			}
			if int64(targetTimestamp) > upperBounds.Unix() {
				continue
			}

			pHistory[targetTimestamp] = pricesValue
		}
	}

	return pHistory, nil
}

func newPricelistHistoryDatabaseV2(dbFilepath string, targetDate time.Time) (PricelistHistoryDatabaseV2, error) {
	logging.WithField("db-filepath", dbFilepath).Info("Opening database")

	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return PricelistHistoryDatabaseV2{}, err
	}

	return PricelistHistoryDatabaseV2{db, targetDate}, nil
}

type PricelistHistoryDatabaseV2 struct {
	db         *bolt.DB
	targetDate time.Time
}

func (phdBase PricelistHistoryDatabaseV2) getItemPriceHistories(itemIds []blizzard.ItemID) chan getItemPriceHistoriesJob {
	// drawing channels
	in := make(chan blizzard.ItemID)
	out := make(chan getItemPriceHistoriesJob)

	// spinning up workers
	worker := func() {
		for itemId := range in {
			history, err := phdBase.getItemPriceHistory(itemId)
			out <- getItemPriceHistoriesJob{err, itemId, history}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// spinning it up
	go func() {
		for _, itemId := range itemIds {
			in <- itemId
		}

		close(in)
	}()

	return out
}

func (phdBase PricelistHistoryDatabaseV2) getItemPriceHistory(itemID blizzard.ItemID) (sotah.PriceHistory, error) {
	out := sotah.PriceHistory{}

	err := phdBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pricelistHistoryBucketName(itemID))
		if bkt == nil {
			return nil
		}

		value := bkt.Get(pricelistHistoryKeyName())
		if value == nil {
			return nil
		}

		var err error
		out, err = sotah.NewPriceHistoryFromBytes(value)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return sotah.PriceHistory{}, err
	}

	return out, nil
}

func (phdBase PricelistHistoryDatabaseV2) persistItemPrices(data map[blizzard.ItemID][]byte) error {
	logging.WithField("items", len(data)).Info("Persisting item-prices")

	err := phdBase.db.Batch(func(tx *bolt.Tx) error {
		for itemId, payload := range data {
			bkt, err := tx.CreateBucketIfNotExists(pricelistHistoryBucketName(itemId))
			if err != nil {
				return err
			}

			if err := bkt.Put(pricelistHistoryKeyName(), payload); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
