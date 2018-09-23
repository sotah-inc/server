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

	for _, reg := range c.filterInRegions(sta.regions) {
		for _, rea := range c.filterInRealms(reg, sta.statuses[reg.Name].Realms) {
			for uTimestamp, dBase := range dBases[reg.Name][rea.Slug] {
				// gathering the db path
				dbPath, err := nextDatabasePath(c, reg, rea, time.Unix(uTimestamp, 0))
				if err != nil {
					return err
				}

				// opening a next database
				nextDbase, err := bolt.Open(dbPath, 0600, nil)
				if err != nil {
					return err
				}

				// going over the items and persisting item pricelist-histories to the next database
				for ID, itemValue := range sta.items {
					plHistory, err := dBase.getPricelistHistory(rea, itemValue.ID)
					if err != nil {
						return err
					}

					err = nextDbase.Batch(func(tx *bolt.Tx) error {
						for itemTimestamp, pricesValue := range plHistory {
							targetDate := time.Unix(itemTimestamp, 0)

							bkt, err := tx.CreateBucketIfNotExists(itemPricelistBucketName(ID))
							if err != nil {
								return err
							}

							encodedPricesValue, err := pricesValue.encodeForPersistence()
							if err != nil {
								return err
							}

							if err := bkt.Put(targetDateToKeyName(targetDate), encodedPricesValue); err != nil {
								return err
							}
						}

						return nil
					})
					if err != nil {
						return err
					}
				}

				// closing out the next database
				if err := nextDbase.Close(); err != nil {
					return err
				}

				// gathering the next database filesize
				nextStat, err := os.Stat(nextDbase.Path())
				if err != nil {
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
	logging.WithField("size", totalNewSize/1000/1000).Info("Total new size")

	return nil
}
