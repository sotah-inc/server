package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	storage "cloud.google.com/go/storage"
	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/objstate"
	"github.com/ihsw/sotah-server/app/util"
)

type unixTimestamp int64

func pricelistHistoryKeyName() []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, 1)

	return key
}

func pricelistHistoryBucketName(ID blizzard.ItemID) []byte {
	return []byte(fmt.Sprintf("item-prices/%d", ID))
}

func pricelistHistoryDatabaseFilePath(c config, rea realm, targetTime time.Time) (string, error) {
	dirPath, err := c.databaseDir()
	if err != nil {
		return "", err
	}

	dbFilePath := fmt.Sprintf(
		"%s/next-%d.db",
		rea.databaseDir(rea.region.databaseDir(dirPath)),
		targetTime.Unix(),
	)
	return dbFilePath, nil
}

func newPricelistHistoryDatabases(c config, regs regionList, stas statuses) (pricelistHistoryDatabases, error) {
	phdBases := pricelistHistoryDatabases{}

	databaseDir, err := c.databaseDir()
	if err != nil {
		return pricelistHistoryDatabases{}, err
	}

	for _, reg := range c.filterInRegions(regs) {
		phdBases[reg.Name] = map[blizzard.RealmSlug]pricelistHistoryDatabaseShards{}

		regionDatabaseDir := reg.databaseDir(databaseDir)

		for _, rea := range c.filterInRealms(reg, stas[reg.Name].Realms) {
			phdBases[reg.Name][rea.Slug] = pricelistHistoryDatabaseShards{}

			realmDatabaseDir := rea.databaseDir(regionDatabaseDir)
			dbPathPairs, err := databasePaths(realmDatabaseDir)
			if err != nil {
				return pricelistHistoryDatabases{}, err
			}

			for _, dbPathPair := range dbPathPairs {
				phdBase, err := newPricelistHistoryDatabase(c, reg, rea, dbPathPair.targetTime)
				if err != nil {
					return pricelistHistoryDatabases{}, err
				}

				phdBases[reg.Name][rea.Slug][dbPathPair.targetTime.Unix()] = phdBase
			}
		}
	}

	return phdBases, nil
}

type pricelistHistoryDatabases map[regionName]map[blizzard.RealmSlug]pricelistHistoryDatabaseShards

func (phdBases pricelistHistoryDatabases) resolveDatabaseFromLoadAuctionsJob(c config, job loadAuctionsJob) (pricelistHistoryDatabase, error) {
	normalizedTargetDate := normalizeTargetDate(job.lastModified)
	phdBase, ok := phdBases[job.realm.region.Name][job.realm.Slug][unixTimestamp(normalizedTargetDate.Unix())]
	if ok {
		return phdBase, nil
	}

	phdBase, err := newPricelistHistoryDatabase(c, job.realm, job.lastModified)
	if err != nil {
		return pricelistHistoryDatabase{}, err
	}
	phdBases[job.realm.region.Name][job.realm.Slug][unixTimestamp(normalizedTargetDate.Unix())] = phdBase

	return phdBase, nil
}

func (phdBases pricelistHistoryDatabases) startLoader(c config, sto store) chan loadAuctionsJob {
	in := make(chan loadAuctionsJob)
	worker := func() {
		for job := range in {
			if job.err != nil {
				logging.WithFields(logrus.Fields{
					"error":  job.err.Error(),
					"region": job.realm.region.Name,
					"realm":  job.realm.Slug,
				}).Error("Erroneous job was passed into pricelist intake channel")

				continue
			}

			phdBase, err := phdBases.resolveDatabaseFromLoadAuctionsJob(c, job)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  job.err.Error(),
					"region": job.realm.region.Name,
					"realm":  job.realm.Slug,
				}).Error("Could not resolve database from load-auctions-job")

				continue
			}

			if err := phdBase.handleLoadAuctionsJob(job, c, sto); err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.realm.region.Name,
					"realm":  job.realm.Slug,
				}).Error("Failed to handle load-auctions-job")

				continue
			}
		}
	}
	postWork := func() {
		return
	}
	util.Work(2, worker, postWork)

	return in
}

func (phdBases pricelistHistoryDatabases) pruneDatabases() error {
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
					"realm":              rSlug,
					"database-timestamp": unixTimestamp,
				}).Debug("Removing database from shard map")
				delete(phdBases[rName][rSlug], unixTimestamp)

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
					return err
				}
			}
		}
	}

	return nil
}

func (phdBases pricelistHistoryDatabases) startPruner(stopChan workerStopChan) workerStopChan {
	onStop := make(workerStopChan)
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

func (phdShards pricelistHistoryDatabaseShards) getPricelistHistory(rea realm, ID blizzard.ItemID) (priceListHistory, error) {
	plHistory := priceListHistory{}

	for _, phdBase := range phdShards {
		receivedHistory, err := phdBase.getPricelistHistory(ID)
		if err != nil {
			return priceListHistory{}, err
		}

		for unixTimestamp, pricesValue := range receivedHistory {
			plHistory[unixTimestamp] = pricesValue
		}
	}

	return plHistory, nil
}

func newPricelistHistoryDatabase(c config, rea realm, targetDate time.Time) (pricelistHistoryDatabase, error) {
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
				"err": job.err.Error(),
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

func (phdBase pricelistHistoryDatabase) persistPricelists(targetTime time.Time, pList priceList) error {
	logging.WithFields(logrus.Fields{
		"target_date": targetTime.Unix(),
		"pricelists":  len(pList),
	}).Debug("Writing pricelists")

	pHistories := phdBase.getPricelistHistories(pList.itemIds())

	err := phdBase.db.Batch(func(tx *bolt.Tx) error {
		for ID, pricesValue := range pList {
			pHistory := func() priceListHistory {
				result, ok := pHistories[ID]
				if !ok {
					return priceListHistory{}
				}

				return result
			}()
			pHistory[targetTime.Unix()] = pricesValue

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

func (phdBase pricelistHistoryDatabase) handleLoadAuctionsJob(job loadAuctionsJob, c config, sto store) error {
	mAuctions := newMiniAuctionListFromBlizzardAuctions(job.auctions.Auctions)
	err := phdBase.persistPricelists(job.lastModified, newPriceList(mAuctions.itemIds(), mAuctions))
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":  err.Error(),
			"region": job.realm.region.Name,
			"realm":  job.realm.Slug,
		}).Error("Failed to persist auctions to pricelist-history database")

		return err
	}

	if c.UseGCloudStorage == false {
		return nil
	}

	bkt := sto.getRealmAuctionsBucket(job.realm)
	obj := bkt.Object(sto.getRealmAuctionsObjectName(job.lastModified))
	objAttrs, err := obj.Attrs(sto.context)
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":         err.Error(),
			"region":        job.realm.region.Name,
			"realm":         job.realm.Slug,
			"last-modified": job.lastModified.Unix(),
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
	if _, err := obj.Update(sto.context, storage.ObjectAttrsToUpdate{Metadata: objMeta}); err != nil {
		logging.WithFields(logrus.Fields{
			"error":         err.Error(),
			"region":        job.realm.region.Name,
			"realm":         job.realm.Slug,
			"last-modified": job.lastModified.Unix(),
		}).Error("Failed to update metadata of object")

		return err
	}

	return nil
}
