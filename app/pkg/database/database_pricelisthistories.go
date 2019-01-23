package database

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/sotah-inc/server/app/pkg/store"

	storage "cloud.google.com/go/storage"
	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/objstate"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/util"
)

func newPriceListHistoryFromBytes(data []byte) (priceListHistory, error) {
	gzipDecoded, err := util.GzipDecode(data)
	if err != nil {
		return priceListHistory{}, err
	}

	out := priceListHistory{}
	if err := json.Unmarshal(gzipDecoded, &out); err != nil {
		return priceListHistory{}, err
	}

	return out, nil
}

type priceListHistory map[unixTimestamp]state.Prices

func (plHistory priceListHistory) encodeForPersistence() ([]byte, error) {
	jsonEncoded, err := json.Marshal(plHistory)
	if err != nil {
		return []byte{}, err
	}

	gzipEncoded, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return []byte{}, err
	}

	return gzipEncoded, nil
}

func pricelistHistoryKeyName() []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, 1)

	return key
}

func pricelistHistoryBucketName(ID blizzard.ItemID) []byte {
	return []byte(fmt.Sprintf("item-prices/%d", ID))
}

func pricelistHistoryDatabaseFilePath(c internal.Config, rea internal.Realm, targetTime time.Time) (string, error) {
	dirPath, err := c.DatabaseDir()
	if err != nil {
		return "", err
	}

	dbFilePath := fmt.Sprintf(
		"%s/next-%d.db",
		rea.DatabaseDir(rea.Region.DatabaseDir(dirPath)),
		targetTime.Unix(),
	)
	return dbFilePath, nil
}

func newPricelistHistoryDatabases(c internal.Config, regs internal.RegionList, stas internal.Statuses) (PricelistHistoryDatabases, error) {
	phdBases := PricelistHistoryDatabases{}

	databaseDir, err := c.DatabaseDir()
	if err != nil {
		return PricelistHistoryDatabases{}, err
	}

	for _, reg := range regs {
		phdBases[reg.Name] = map[blizzard.RealmSlug]pricelistHistoryDatabaseShards{}

		regionDatabaseDir := reg.DatabaseDir(databaseDir)

		for _, rea := range stas[reg.Name].Realms {
			phdBases[reg.Name][rea.Slug] = pricelistHistoryDatabaseShards{}

			realmDatabaseDir := rea.DatabaseDir(regionDatabaseDir)
			dbPathPairs, err := databasePaths(realmDatabaseDir)
			if err != nil {
				return PricelistHistoryDatabases{}, err
			}

			for _, dbPathPair := range dbPathPairs {
				phdBase, err := newPricelistHistoryDatabase(c, rea, dbPathPair.targetTime)
				if err != nil {
					return PricelistHistoryDatabases{}, err
				}

				phdBases[reg.Name][rea.Slug][unixTimestamp(dbPathPair.targetTime.Unix())] = phdBase
			}
		}
	}

	return phdBases, nil
}

type PricelistHistoryDatabases map[internal.RegionName]map[blizzard.RealmSlug]pricelistHistoryDatabaseShards

func (phdBases PricelistHistoryDatabases) resolveDatabaseFromLoadAuctionsJob(c internal.Config, job internal.LoadAuctionsJob) (pricelistHistoryDatabase, error) {
	normalizedTargetDate := normalizeTargetDate(job.LastModified)
	phdBase, ok := phdBases[job.Realm.Region.Name][job.Realm.Slug][unixTimestamp(normalizedTargetDate.Unix())]
	if ok {
		return phdBase, nil
	}

	phdBase, err := newPricelistHistoryDatabase(c, job.Realm, job.LastModified)
	if err != nil {
		return pricelistHistoryDatabase{}, err
	}
	phdBases[job.Realm.Region.Name][job.Realm.Slug][unixTimestamp(normalizedTargetDate.Unix())] = phdBase

	return phdBase, nil
}

func (phdBases PricelistHistoryDatabases) Load(in chan internal.LoadAuctionsJob, c internal.Config, sto store.Store) chan struct{} {
	done := make(chan struct{})

	worker := func() {
		for job := range in {
			if job.Err != nil {
				logging.WithFields(logrus.Fields{
					"error":  job.Err.Error(),
					"region": job.Realm.Region.Name,
					"Realm":  job.Realm.Slug,
				}).Error("Erroneous job was passed into pricelist intake channel")

				continue
			}

			phdBase, err := phdBases.resolveDatabaseFromLoadAuctionsJob(c, job)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  job.Err.Error(),
					"region": job.Realm.Region.Name,
					"Realm":  job.Realm.Slug,
				}).Error("Could not resolve database from Load-auctions-job")

				continue
			}

			if err := phdBase.handleLoadAuctionsJob(job, c, sto); err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.Realm.Region.Name,
					"Realm":  job.Realm.Slug,
				}).Error("Failed to handle Load-auctions-job")

				continue
			}
		}
	}
	postWork := func() {
		done <- struct{}{}

		return
	}
	util.Work(2, worker, postWork)

	return done
}

func (phdBases PricelistHistoryDatabases) pruneDatabases() error {
	earliestUnixTimestamp := databaseRetentionLimit().Unix()
	logging.WithField("limit", earliestUnixTimestamp).Info("Checking for databases to prune")
	for rName, realmDatabases := range phdBases {
		for rSlug, databaseShards := range realmDatabases {
			for unixTimestamp, phdBase := range databaseShards {
				if int64(unixTimestamp) > earliestUnixTimestamp {
					continue
				}

				logging.WithFields(logrus.Fields{
					"region":             rName,
					"Realm":              rSlug,
					"database-timestamp": unixTimestamp,
				}).Debug("Removing database from shard map")
				delete(phdBases[rName][rSlug], unixTimestamp)

				dbPath := phdBase.db.Path()

				logging.WithFields(logrus.Fields{
					"region":             rName,
					"Realm":              rSlug,
					"database-timestamp": unixTimestamp,
				}).Debug("Closing database")
				if err := phdBase.db.Close(); err != nil {
					logging.WithFields(logrus.Fields{
						"region":   rName,
						"Realm":    rSlug,
						"database": dbPath,
					}).Error("Failed to close database")

					return err
				}

				logging.WithFields(logrus.Fields{
					"region":   rName,
					"Realm":    rSlug,
					"filepath": dbPath,
				}).Debug("Deleting database file")
				if err := os.Remove(dbPath); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (phdBases PricelistHistoryDatabases) startPruner(stopChan state.WorkerStopChan) state.WorkerStopChan {
	onStop := make(state.WorkerStopChan)
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

type pricelistHistoryDatabaseShards map[unixTimestamp]pricelistHistoryDatabase

func (phdShards pricelistHistoryDatabaseShards) getPricelistHistory(
	rea internal.Realm,
	ID blizzard.ItemID,
	lowerBounds time.Time,
	upperBounds time.Time,
) (priceListHistory, error) {
	plHistory := priceListHistory{}

	for _, phdBase := range phdShards {
		receivedHistory, err := phdBase.getPricelistHistory(ID)
		if err != nil {
			return priceListHistory{}, err
		}

		for uTimestamp, pricesValue := range receivedHistory {
			if int64(uTimestamp) < lowerBounds.Unix() {
				continue
			}
			if int64(uTimestamp) > upperBounds.Unix() {
				continue
			}

			plHistory[uTimestamp] = pricesValue
		}
	}

	return plHistory, nil
}

func newPricelistHistoryDatabase(c internal.Config, rea internal.Realm, targetDate time.Time) (pricelistHistoryDatabase, error) {
	dbFilepath, err := pricelistHistoryDatabaseFilePath(c, rea, targetDate)
	if err != nil {
		return pricelistHistoryDatabase{}, err
	}

	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return pricelistHistoryDatabase{}, err
	}

	return pricelistHistoryDatabase{db, targetDate}, nil
}

type pricelistHistoryDatabase struct {
	db         *bolt.DB
	targetDate time.Time
}

type getPricelistHistoriesJob struct {
	err     error
	ID      blizzard.ItemID
	history priceListHistory
}

func (phdBase pricelistHistoryDatabase) getPricelistHistories(IDs []blizzard.ItemID) map[blizzard.ItemID]priceListHistory {
	// spawning workers
	in := make(chan blizzard.ItemID)
	out := make(chan getPricelistHistoriesJob)
	worker := func() {
		for ID := range in {
			history, err := phdBase.getPricelistHistory(ID)
			out <- getPricelistHistoriesJob{err, ID, history}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// spinning it up
	go func() {
		for _, ID := range IDs {
			in <- ID
		}

		close(in)
	}()

	// going over results
	results := map[blizzard.ItemID]priceListHistory{}
	for job := range out {
		if job.err != nil {
			logging.WithFields(logrus.Fields{
				"Err": job.err.Error(),
				"ID":  job.ID,
			}).Error("Failed to get pricelist-history")

			continue
		}

		results[job.ID] = job.history
	}

	return results
}

func (phdBase pricelistHistoryDatabase) getPricelistHistory(ID blizzard.ItemID) (priceListHistory, error) {
	out := priceListHistory{}
	err := phdBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pricelistHistoryBucketName(ID))
		if bkt == nil {
			return nil
		}

		value := bkt.Get(pricelistHistoryKeyName())
		if value == nil {
			return nil
		}

		var err error
		out, err = newPriceListHistoryFromBytes(value)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return priceListHistory{}, err
	}

	return out, nil
}

func (phdBase pricelistHistoryDatabase) persistPricelists(targetTime time.Time, pList state.PriceList) error {
	logging.WithFields(logrus.Fields{
		"target_date": targetTime.Unix(),
		"pricelists":  len(pList),
	}).Debug("Writing pricelists")

	pHistories := phdBase.getPricelistHistories(pList.ItemIds())

	err := phdBase.db.Batch(func(tx *bolt.Tx) error {
		for ID, pricesValue := range pList {
			pHistory := func() priceListHistory {
				result, ok := pHistories[ID]
				if !ok {
					return priceListHistory{}
				}

				return result
			}()
			pHistory[unixTimestamp(targetTime.Unix())] = pricesValue

			bkt, err := tx.CreateBucketIfNotExists(pricelistHistoryBucketName(ID))
			if err != nil {
				return err
			}

			encodedValue, err := pHistory.encodeForPersistence()
			if err != nil {
				return err
			}

			if err := bkt.Put(pricelistHistoryKeyName(), encodedValue); err != nil {
				return err
			}
		}

		logging.WithFields(logrus.Fields{
			"target_date": phdBase.targetDate.Unix(),
			"pricelists":  len(pList),
		}).Debug("Finished writing pricelists")

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (phdBase pricelistHistoryDatabase) handleLoadAuctionsJob(job internal.LoadAuctionsJob, c internal.Config, sto store.Store) error {
	mAuctions := internal.NewMiniAuctionListFromBlizzardAuctions(job.Auctions.Auctions)
	err := phdBase.persistPricelists(job.LastModified, state.NewPriceList(mAuctions.ItemIds(), mAuctions))
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":  err.Error(),
			"region": job.Realm.Region.Name,
			"Realm":  job.Realm.Slug,
		}).Error("Failed to persist auctions to pricelist-history database")

		return err
	}

	if c.UseGCloud == false {
		return nil
	}

	bkt := sto.GetRealmAuctionsBucket(job.Realm)
	obj := bkt.Object(sto.GetRealmAuctionsObjectName(job.LastModified))
	objAttrs, err := obj.Attrs(sto.Context)
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":         err.Error(),
			"region":        job.Realm.Region.Name,
			"Realm":         job.Realm.Slug,
			"last-modified": job.LastModified.Unix(),
		}).Error("Failed to fetch obj attrs")

		return err
	}

	objMeta := func() map[string]string {
		if objAttrs.Metadata == nil {
			return map[string]string{}
		}

		return objAttrs.Metadata
	}()
	objMeta["state"] = string(objstate.Processed)
	if _, err := obj.Update(sto.Context, storage.ObjectAttrsToUpdate{Metadata: objMeta}); err != nil {
		logging.WithFields(logrus.Fields{
			"error":         err.Error(),
			"region":        job.Realm.Region.Name,
			"Realm":         job.Realm.Slug,
			"last-modified": job.LastModified.Unix(),
		}).Error("Failed to update metadata of object")

		return err
	}

	return nil
}
