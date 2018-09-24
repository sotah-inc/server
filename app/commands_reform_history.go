package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"

	"github.com/ihsw/sotah-server/app/blizzard"

	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/util"
)

type unixTimestamp int64

type pricelistHistoryDatabaseSizes struct {
	region      region
	realm       realm
	targetDate  time.Time
	currentSize int64
	currentPath string
	newSize     int64
	newPath     string
}

func batchPersistParallel(nextDbase *bolt.DB, uTimestamp int64, in chan priceListHistoryJob) error {
	err := nextDbase.Batch(func(tx *bolt.Tx) error {
		done := make(chan struct{})

		// spinning up workers for pricelist-history job intake
		worker := func() {
			for plhJob := range in {
				if plhJob.err != nil {
					logging.WithFields(logrus.Fields{
						"error": plhJob.err.Error(),
						"id":    plhJob.ID,
					}).Error("Failed to fetch pricelist-history")

					continue
				}

				if len(plhJob.history) == 0 {
					continue
				}

				bucketName := itemPricelistBucketName(plhJob.ID)

				bkt, err := tx.CreateBucketIfNotExists(bucketName)
				if err != nil {
					logging.WithField("error", err.Error()).Error("Failed to create bucket")

					continue
				}

				encodedPricesValue, err := plhJob.history.encodeForPersistence()
				if err != nil {
					logging.WithField("error", err.Error()).Error("Failed to encode history")

					continue
				}

				logging.WithFields(logrus.Fields{
					"item":   plhJob.ID,
					"bucket": string(bucketName),
					"key":    uTimestamp,
					"prices": len(plhJob.history),
				}).Debug("Writing histories")

				if err := bkt.Put(targetDateToKeyName(time.Unix(uTimestamp, 0)), encodedPricesValue); err != nil {
					logging.WithField("error", err.Error()).Error("Failed to write to bucket")

					continue
				}
			}
		}
		postWork := func() {
			done <- struct{}{}

			return
		}
		util.Work(4, worker, postWork)

		// waiting for them to finish out
		<-done

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func reformHistory(c config, m messenger, s store) error {
	logging.Info("Starting reform-history")

	// establishing a state
	res := newResolver(c, m, s)
	sta := newState(m, res)

	// gathering region-status from the root service
	logging.Info("Gathering regions")
	regions := []*region{}
	attempts := 0
	for {
		var err error
		regions, err = newRegionsFromMessenger(m)
		if err == nil {
			break
		} else {
			logging.Info("Could not fetch regions, retrying in 250ms")

			attempts++
			time.Sleep(250 * time.Millisecond)
		}

		if attempts >= 20 {
			return fmt.Errorf("Failed to fetch regions after %d attempts", attempts)
		}
	}

	for i, reg := range regions {
		sta.regions[i] = *reg
	}

	// filling state with statuses
	logging.Info("Gathering statuses")
	for _, reg := range c.filterInRegions(sta.regions) {
		regionStatus, err := newStatusFromMessenger(reg, m)
		if err != nil {
			logging.WithField("region", reg.Name).Info("Could not fetch status for region")

			return err
		}

		sta.statuses[reg.Name] = regionStatus
	}

	// loading up items
	logging.Info("Loading items")
	loadedItems, err := loadItemsFromFilecache(*res.config)
	if err != nil {
		return err
	}
	for job := range loadedItems {
		if job.err != nil {
			logging.WithFields(logrus.Fields{
				"error":    job.err.Error(),
				"filepath": job.filepath,
			}).Error("Failed to load item")

			return job.err
		}

		sta.items[job.item.ID] = item{job.item, job.iconURL}
	}

	// validating database-dirs exist
	logging.Info("Validating database-dirs exist")
	databaseDir, err := c.databaseDir()
	if err != nil {
		return err
	}
	exists, err := util.StatExists(databaseDir)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("Database dir does not exist")
	}
	for _, reg := range c.filterInRegions(sta.regions) {
		regionDatabaseDir := reg.databaseDir(databaseDir)
		exists, err := util.StatExists(regionDatabaseDir)
		if err != nil {
			return err
		}
		if !exists {
			return errors.New("Region dir does not exist")
		}

		for _, rea := range c.filterInRealms(reg, sta.statuses[reg.Name].Realms) {
			exists, err := util.StatExists(rea.databaseDir(regionDatabaseDir))
			if err != nil {
				return err
			}
			if !exists {
				return errors.New("Realm dir does not exist")
			}
		}
	}

	// loading up databases
	logging.Info("Loading databases")
	dBases, err := newDatabases(c, sta.regions, sta.statuses)
	if err != nil {
		return err
	}

	// initializing current-sizes
	currentSizes := map[regionName]map[blizzard.RealmSlug]map[unixTimestamp]pricelistHistoryDatabaseSizes{}
	for _, reg := range c.filterInRegions(sta.regions) {
		currentSizes[reg.Name] = map[blizzard.RealmSlug]map[unixTimestamp]pricelistHistoryDatabaseSizes{}

		for _, rea := range c.filterInRealms(reg, sta.statuses[reg.Name].Realms) {
			currentSizes[reg.Name][rea.Slug] = map[unixTimestamp]pricelistHistoryDatabaseSizes{}

			for _, dBase := range dBases[reg.Name][rea.Slug] {
				currentSizes[reg.Name][rea.Slug][unixTimestamp(dBase.targetDate.Unix())] = pricelistHistoryDatabaseSizes{
					region:      reg,
					realm:       rea,
					targetDate:  dBase.targetDate,
					currentPath: dBase.db.Path(),
					currentSize: 0,
					newPath:     "",
					newSize:     0,
				}
			}
		}
	}

	// spinning up workers for gathering all database sizes
	logging.Info("Gathering all database file sizes")
	sizeResults := func() chan pricelistHistoryDatabaseSizes {
		inChan := make(chan pricelistHistoryDatabaseSizes)
		outChan := make(chan pricelistHistoryDatabaseSizes)
		worker := func() {
			for size := range inChan {
				stat, err := os.Stat(size.currentPath)
				if err != nil {
					logging.WithField("error", err.Error()).Error("Could not stat file")

					continue
				}

				size.currentSize = stat.Size()
				outChan <- size
			}
		}
		postWork := func() {
			close(outChan)
		}
		util.Work(4, worker, postWork)

		go func() {
			for _, realmSizes := range currentSizes {
				for _, targetTimeSizes := range realmSizes {
					for _, size := range targetTimeSizes {
						inChan <- size
					}
				}
			}

			close(inChan)
		}()

		return outChan
	}()

	// processing size intake
	for size := range sizeResults {
		currentSizes[size.region.Name][size.realm.Slug][unixTimestamp(size.targetDate.Unix())] = size
	}
	totalSize := func() float64 {
		result := int64(0)
		for _, realmSizes := range currentSizes {
			for _, targetTimeSizes := range realmSizes {
				for _, size := range targetTimeSizes {
					result += size.currentSize
				}
			}
		}

		return float64(result)
	}()
	logging.WithField("size", totalSize/1000/1000).Info("Total size")

	logging.Info("Recalculating against new format")
	for _, reg := range c.filterInRegions(sta.regions) {
		logging.WithField("region", reg.Name).Debug("Going over region")

		for _, rea := range c.filterInRealms(reg, sta.statuses[reg.Name].Realms) {
			logging.WithFields(logrus.Fields{
				"region": reg.Name,
				"realm":  rea.Slug,
			}).Debug("Going over realm")

			for uTimestamp, dBase := range dBases[reg.Name][rea.Slug] {
				logging.WithFields(logrus.Fields{
					"region":      reg.Name,
					"realm":       rea.Slug,
					"target-date": uTimestamp,
				}).Debug("Going over database at time")

				// gathering the db path
				nextDbPath, err := nextDatabasePath(c, reg, rea, time.Unix(uTimestamp, 0))
				if err != nil {
					return err
				}

				// opening a next database
				nextDbase, err := bolt.Open(nextDbPath, 0600, nil)
				if err != nil {
					return err
				}

				// going over the items and persisting item pricelist-histories to the next database
				logging.WithFields(logrus.Fields{
					"region":      reg.Name,
					"realm":       rea.Slug,
					"target-date": uTimestamp,
					"items":       len(sta.items.getItemIds()),
				}).Debug("Gathering pricelist histories at time")
				err = batchPersistParallel(nextDbase, uTimestamp, dBase.getPricelistHistories(sta.items.getItemIds()))
				if err != nil {
					logging.WithFields(logrus.Fields{"error": err.Error()}).Error("Failed to batch persist")

					return err
				}

				// closing out the next database
				logging.WithFields(logrus.Fields{
					"region":      reg.Name,
					"realm":       rea.Slug,
					"target-date": uTimestamp,
				}).Debug("Closing out next dbase file")
				if err := nextDbase.Close(); err != nil {
					return err
				}

				// gathering the next database filesize
				logging.WithFields(logrus.Fields{
					"region":      reg.Name,
					"realm":       rea.Slug,
					"target-date": uTimestamp,
					"path":        nextDbPath,
				}).Debug("Stating next dbase file")
				nextStat, err := os.Stat(nextDbPath)
				if err != nil {
					return err
				}

				// removing unused file
				logging.WithFields(logrus.Fields{
					"region":      reg.Name,
					"realm":       rea.Slug,
					"target-date": uTimestamp,
				}).Debug("Removing next dbase file")
				if err := os.Remove(nextDbPath); err != nil {
					return err
				}

				size := currentSizes[reg.Name][rea.Slug][unixTimestamp(uTimestamp)]
				size.newPath = nextDbase.Path()
				size.newSize = nextStat.Size()
				currentSizes[reg.Name][rea.Slug][unixTimestamp(uTimestamp)] = size
			}
		}
	}

	totalNewSize := func() float64 {
		result := int64(0)
		for _, realmSizes := range currentSizes {
			for _, targetTimeSizes := range realmSizes {
				for _, size := range targetTimeSizes {
					result += size.newSize
				}
			}
		}

		return float64(result)
	}()
	logging.WithFields(logrus.Fields{
		"current-size": totalSize / 1000 / 1000,
		"new-size":     totalNewSize / 1000 / 1000,
	}).Info("Total sizes")

	return nil
}
