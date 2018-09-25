package main

import (
	"fmt"
	"os"
	"time"

	"github.com/boltdb/bolt"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/util"
	"github.com/sirupsen/logrus"
)

func pricelistHistoryDatabaseFilePath(c config, rea realm, targetTime time.Time) (string, error) {
	dirPath, err := c.databaseDir()
	if err != nil {
		return "", err
	}

	dbFilePath := fmt.Sprintf(
		"%s/next-%d.db",
		rea.databaseDir(rea.region.databaseDir(dirPath)),
		time.Unix(),
	)
	return dbFilePath, nil
}

func newPricelistHistoryDatabase(c config, rea realm, targetDate time.Time) (database, error) {
	dbFilepath, err := pricelistHistoryDatabaseFilePath(c, rea, targetDate)
	if err != nil {
		return database{}, err
	}

	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return database{}, err
	}

	return database{db, targetDate}, nil
}

type pricelistHistoryDatabase struct {
	db         *bolt.DB
	targetDate time.Time
}

type pricelistHistoryDatabases map[regionName]map[blizzard.RealmSlug]pricelistHistoryDatabaseShards

func (dBases databases) resolveDatabaseFromLoadAuctionsJob(c config, job loadAuctionsJob) (database, error) {
	normalizedTargetDate := normalizeTargetDate(job.lastModified)
	dBase, ok := dBases[job.realm.region.Name][job.realm.Slug][normalizedTargetDate.Unix()]
	if ok {
		return dBase, nil
	}

	dBase, err := newDatabase(c, job.realm.region, job.realm, job.lastModified)
	if err != nil {
		return database{}, err
	}
	dBases[job.realm.region.Name][job.realm.Slug][normalizedTargetDate.Unix()] = dBase

	return dBase, nil
}

func (dBases databases) startLoader(c config, sto store) chan loadAuctionsJob {
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

			dBase, err := dBases.resolveDatabaseFromLoadAuctionsJob(c, job)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  job.err.Error(),
					"region": job.realm.region.Name,
					"realm":  job.realm.Slug,
				}).Error("Could not resolve database from load-auctions-job")

				continue
			}

			if err := dBase.handleLoadAuctionsJob(job, c, sto); err != nil {
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

func (dBases databases) startPruner(stopChan workerStopChan) workerStopChan {
	onStop := make(workerStopChan)
	go func() {
		ticker := time.NewTicker(20 * time.Minute)

		logging.Info("Starting pruner")
	outer:
		for {
			select {
			case <-ticker.C:
				if err := dBases.pruneDatabases(); err != nil {
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

func (dBases databases) pruneDatabases() error {
	earliestUnixTimestamp := databaseRetentionLimit().Unix()
	logging.WithField("limit", earliestUnixTimestamp).Info("Checking for databases to prune")
	for rName, realmDatabases := range dBases {
		for rSlug, timestampDatabases := range realmDatabases {
			for unixTimestamp, dBase := range timestampDatabases {
				if unixTimestamp > earliestUnixTimestamp {
					continue
				}

				logging.WithFields(logrus.Fields{
					"region":             rName,
					"realm":              rSlug,
					"database-timestamp": unixTimestamp,
				}).Debug("Removing database from shard map")
				delete(dBases[rName][rSlug], unixTimestamp)

				dbPath := dBase.db.Path()

				logging.WithFields(logrus.Fields{
					"region":             rName,
					"realm":              rSlug,
					"database-timestamp": unixTimestamp,
				}).Debug("Closing database")
				if err := dBase.db.Close(); err != nil {
					logging.WithFields(logrus.Fields{
						"region":   rName,
						"realm":    rSlug,
						"database": dBase.db.Path(),
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

type pricelistHistoryDatabaseShards map[unixTimestamp]pricelistHistoryDatabase

func (phdShards pricelistHistoryDatabaseShards) getPricelistHistory(rea realm, ID blizzard.ItemID) (priceListHistory, error) {
	plHistory := priceListHistory{}

	for _, dBase := range tdMap {
		receivedHistory, err := dBase.getPricelistHistory(ID)
		if err != nil {
			return priceListHistory{}, err
		}

		for unixTimestamp, pricesValue := range receivedHistory {
			plHistory[unixTimestamp] = pricesValue
		}
	}

	return plHistory, nil
}
