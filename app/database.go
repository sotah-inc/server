package main

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	storage "cloud.google.com/go/storage"
	"github.com/boltdb/bolt"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/objstate"
	"github.com/ihsw/sotah-server/app/util"
	"github.com/sirupsen/logrus"
)

func targetDateToKeyName(targetDate time.Time) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(targetDate.Unix()))

	return key
}

func keyNameToTargetDate(key []byte) time.Time {
	return time.Unix(int64(binary.LittleEndian.Uint64(key)), 0)
}

func itemPricelistBucketName(ID blizzard.ItemID) []byte {
	return []byte(fmt.Sprintf("item-prices/%d", ID))
}

func normalizeTargetDate(targetDate time.Time) time.Time {
	nearestWeekStartOffset := targetDate.Second() + targetDate.Minute()*60 + targetDate.Hour()*60*60 + int(targetDate.Weekday())*60*60*24
	return time.Unix(targetDate.Unix()-int64(nearestWeekStartOffset), 0)
}

func databasePath(c config, reg region, rea realm, targetDate time.Time) (string, error) {
	return filepath.Abs(
		fmt.Sprintf("%s/databases/%s/%s/%d.db", c.CacheDir, reg.Name, rea.Slug, normalizeTargetDate(targetDate).Unix()),
	)
}

func newDatabase(c config, reg region, rea realm, targetDate time.Time) (database, error) {
	dbFilepath, err := databasePath(c, reg, rea, targetDate)
	if err != nil {
		return database{}, err
	}

	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return database{}, err
	}

	return database{db, targetDate}, nil
}

type database struct {
	db         *bolt.DB
	targetDate time.Time
}

func (dBase database) handleLoadAuctionsJob(job loadAuctionsJob, c config, sto store) error {
	mAuctions := newMiniAuctionListFromBlizzardAuctions(job.auctions.Auctions)
	err := dBase.persistPricelists(newPriceList(mAuctions.itemIds(), mAuctions))
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":  err.Error(),
			"region": job.realm.region.Name,
			"realm":  job.realm.Slug,
		}).Error("Failed to persist auctions to database")

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

func (dBase database) persistPricelists(pList priceList) error {
	logging.WithFields(logrus.Fields{
		"target_date": dBase.targetDate.Unix(),
		"pricelists":  len(pList),
	}).Debug("Writing pricelists")

	err := dBase.db.Batch(func(tx *bolt.Tx) error {
		for ID, pricesValue := range pList {
			bkt, err := tx.CreateBucketIfNotExists(itemPricelistBucketName(ID))
			if err != nil {
				return err
			}

			encodedPricesValue, err := pricesValue.encodeForPersistence()
			if err != nil {
				return err
			}

			if err := bkt.Put(targetDateToKeyName(dBase.targetDate), encodedPricesValue); err != nil {
				return err
			}
		}

		logging.WithFields(logrus.Fields{
			"target_date": dBase.targetDate.Unix(),
			"pricelists":  len(pList),
		}).Debug("Finished writing pricelists")

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

type priceListHistory map[int64]prices

func (dBase database) getPricelistHistory(rea realm, ID blizzard.ItemID) (priceListHistory, error) {
	plHistory := priceListHistory{}
	err := dBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(itemPricelistBucketName(ID))
		if bkt == nil {
			logging.WithFields(logrus.Fields{
				"region":      rea.region.Name,
				"realm":       rea.Slug,
				"bucket-name": string(itemPricelistBucketName(ID)),
			}).Debug("Bucket not found")

			return nil
		}

		return bkt.ForEach(func(k, v []byte) error {
			targetDate := keyNameToTargetDate(k)
			pricesValue, err := newPricesFromBytes(v)
			if err != nil {
				return err
			}

			plHistory[targetDate.Unix()] = pricesValue

			return nil
		})
	})
	if err != nil {
		return priceListHistory{}, err
	}

	return plHistory, nil
}

func newDatabases(c config, regs regionList, stas statuses) (databases, error) {
	dBases := map[regionName]map[blizzard.RealmSlug]timestampDatabaseMap{}

	databaseDir, err := c.databaseDir()
	if err != nil {
		return databases{}, err
	}

	for _, reg := range c.filterInRegions(regs) {
		dBases[reg.Name] = map[blizzard.RealmSlug]timestampDatabaseMap{}

		regionDatabaseDir := reg.databaseDir(databaseDir)

		for _, rea := range c.filterInRealms(reg, stas[reg.Name].Realms) {
			dBases[reg.Name][rea.Slug] = timestampDatabaseMap{}

			realmDatabaseDir := rea.databaseDir(regionDatabaseDir)
			databaseFilepaths, err := ioutil.ReadDir(realmDatabaseDir)
			if err != nil {
				return databases{}, err
			}

			for _, fPath := range databaseFilepaths {
				parts := strings.Split(fPath.Name(), ".")
				targetTimeUnix, err := strconv.Atoi(parts[0])
				if err != nil {
					return databases{}, err
				}

				targetTime := time.Unix(int64(targetTimeUnix), 0)
				dBase, err := newDatabase(c, reg, rea, targetTime)
				if err != nil {
					return databases{}, err
				}

				dBases[reg.Name][rea.Slug][targetTime.Unix()] = dBase
			}
		}
	}

	return dBases, nil
}

type databases map[regionName]map[blizzard.RealmSlug]timestampDatabaseMap

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
	util.Work(4, worker, postWork)

	return in
}

type timestampDatabaseMap map[int64]database

// todo: create func for gathering pricelist history across all shards for a given item
