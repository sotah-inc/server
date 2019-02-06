package database

import (
	"encoding/binary"
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

// keying
func pricelistHistoryKeyName() []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, 1)

	return key
}

// bucketing
func pricelistHistoryBucketName(ID blizzard.ItemID) []byte {
	return []byte(fmt.Sprintf("item-prices/%d", ID))
}

// db
func pricelistHistoryDatabaseFilePath(dirPath string, rea sotah.Realm, targetTime time.Time) string {
	return fmt.Sprintf(
		"%s/%s/%s/next-%d.db",
		dirPath,
		rea.Region.Name,
		rea.Slug,
		targetTime.Unix(),
	)
}

func NewPricelistHistoryDatabases(dirPath string, statuses sotah.Statuses) (PricelistHistoryDatabases, error) {
	if len(dirPath) == 0 {
		return PricelistHistoryDatabases{}, errors.New("dir-path cannot be blank")
	}

	phdBases := PricelistHistoryDatabases{
		databaseDir: dirPath,
		Databases:   regionRealmDatabaseShards{},
	}

	for regionName, regionStatuses := range statuses {
		phdBases.Databases[regionName] = realmDatabaseShards{}

		for _, rea := range regionStatuses.Realms {
			phdBases.Databases[regionName][rea.Slug] = PricelistHistoryDatabaseShards{}

			dbPathPairs, err := DatabasePaths(fmt.Sprintf("%s/%s/%s", dirPath, regionName, rea.Slug))
			if err != nil {
				return PricelistHistoryDatabases{}, err
			}

			for _, dbPathPair := range dbPathPairs {
				phdBase, err := newPricelistHistoryDatabase(dbPathPair.FullPath, dbPathPair.TargetTime)
				if err != nil {
					return PricelistHistoryDatabases{}, err
				}

				phdBases.Databases[regionName][rea.Slug][sotah.UnixTimestamp(dbPathPair.TargetTime.Unix())] = phdBase
			}
		}
	}

	return phdBases, nil
}

type regionRealmDatabaseShards map[blizzard.RegionName]realmDatabaseShards

type realmDatabaseShards map[blizzard.RealmSlug]PricelistHistoryDatabaseShards

type PricelistHistoryDatabases struct {
	databaseDir string
	Databases   regionRealmDatabaseShards
}

func (phdBases PricelistHistoryDatabases) resolveDatabaseFromLoadInJob(job LoadInJob) (PricelistHistoryDatabase, error) {
	normalizedTargetDate := normalizeTargetDate(job.TargetTime)
	normalizedTargetTimestamp := sotah.UnixTimestamp(normalizedTargetDate.Unix())

	phdBase, ok := phdBases.Databases[job.Realm.Region.Name][job.Realm.Slug][normalizedTargetTimestamp]
	if ok {
		return phdBase, nil
	}

	dbPath := pricelistHistoryDatabaseFilePath(phdBases.databaseDir, job.Realm, normalizedTargetDate)
	phdBase, err := newPricelistHistoryDatabase(dbPath, normalizedTargetDate)
	if err != nil {
		return PricelistHistoryDatabase{}, err
	}
	phdBases.Databases[job.Realm.Region.Name][job.Realm.Slug][normalizedTargetTimestamp] = phdBase

	return phdBase, nil
}

type pricelistHistoriesLoadOutJob struct {
	Err          error
	Realm        sotah.Realm
	LastModified time.Time
}

func (job pricelistHistoriesLoadOutJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":         job.Err.Error(),
		"region":        job.Realm.Region.Name,
		"realm":         job.Realm.Slug,
		"last-modified": job.LastModified.Unix(),
	}
}

func (phdBases PricelistHistoryDatabases) Load(in chan LoadInJob) chan pricelistHistoriesLoadOutJob {
	// establishing channels
	out := make(chan pricelistHistoriesLoadOutJob)

	// spinning up workers for receiving auctions and persisting them
	worker := func() {
		for job := range in {
			phdBase, err := phdBases.resolveDatabaseFromLoadInJob(job)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.Realm.Region.Name,
					"realm":  job.Realm.Slug,
				}).Error("Could not resolve database from load job")

				out <- pricelistHistoriesLoadOutJob{
					Err:          err,
					Realm:        job.Realm,
					LastModified: job.TargetTime,
				}

				continue
			}

			iPrices := sotah.NewItemPrices(sotah.NewMiniAuctionListFromMiniAuctions(sotah.NewMiniAuctions(job.Auctions)))
			if err := phdBase.persistItemPrices(job.TargetTime, iPrices); err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.Realm.Region.Name,
					"realm":  job.Realm.Slug,
				}).Error("Failed to persist pricelists")

				out <- pricelistHistoriesLoadOutJob{
					Err:          err,
					Realm:        job.Realm,
					LastModified: job.TargetTime,
				}

				continue
			}

			out <- pricelistHistoriesLoadOutJob{
				Err:          nil,
				Realm:        job.Realm,
				LastModified: job.TargetTime,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	return out
}

func (phdBases PricelistHistoryDatabases) pruneDatabases() error {
	earliestUnixTimestamp := DatabaseRetentionLimit().Unix()
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

func (phdBases PricelistHistoryDatabases) StartPruner(stopChan sotah.WorkerStopChan) sotah.WorkerStopChan {
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

type PricelistHistoryDatabaseShards map[sotah.UnixTimestamp]PricelistHistoryDatabase

func (phdShards PricelistHistoryDatabaseShards) GetPriceHistory(
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

func newPricelistHistoryDatabase(dbFilepath string, targetDate time.Time) (PricelistHistoryDatabase, error) {
	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return PricelistHistoryDatabase{}, err
	}

	return PricelistHistoryDatabase{db, targetDate}, nil
}

type PricelistHistoryDatabase struct {
	db         *bolt.DB
	targetDate time.Time
}

// gathering item-price-histories
type getItemPriceHistoriesJob struct {
	err     error
	ItemID  blizzard.ItemID
	history sotah.PriceHistory
}

func (phdBase PricelistHistoryDatabase) getItemPriceHistories(itemIds []blizzard.ItemID) chan getItemPriceHistoriesJob {
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

func (phdBase PricelistHistoryDatabase) getItemPriceHistory(itemID blizzard.ItemID) (sotah.PriceHistory, error) {
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

func (phdBase PricelistHistoryDatabase) persistItemPrices(targetTime time.Time, iPrices sotah.ItemPrices) error {
	targetTimestamp := sotah.UnixTimestamp(targetTime.Unix())

	logging.WithFields(logrus.Fields{
		"target-date": targetTimestamp,
		"item-prices": len(iPrices),
	}).Debug("Writing item-prices")

	ipHistories := sotah.ItemPriceHistories{}
	for job := range phdBase.getItemPriceHistories(iPrices.ItemIds()) {
		if job.err != nil {
			return job.err
		}

		ipHistories[job.ItemID] = job.history
	}

	err := phdBase.db.Batch(func(tx *bolt.Tx) error {
		for ItemID, pricesValue := range iPrices {
			pHistory := func() sotah.PriceHistory {
				result, ok := ipHistories[ItemID]
				if !ok {
					return sotah.PriceHistory{}
				}

				return result
			}()
			pHistory[targetTimestamp] = pricesValue

			bkt, err := tx.CreateBucketIfNotExists(pricelistHistoryBucketName(ItemID))
			if err != nil {
				return err
			}

			encodedValue, err := pHistory.EncodeForPersistence()
			if err != nil {
				return err
			}

			if err := bkt.Put(pricelistHistoryKeyName(), encodedValue); err != nil {
				return err
			}
		}

		logging.WithFields(logrus.Fields{
			"target-date": targetTimestamp,
			"item-prices": len(iPrices),
		}).Debug("Finished writing item-price-histories")

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
