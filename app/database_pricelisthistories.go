package main

import (
	"errors"
	"fmt"
	"time"

	storage "cloud.google.com/go/storage"
	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/objstate"
	"github.com/ihsw/sotah-server/app/util"
)

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

func (phdBase pricelistHistoryDatabase) getPricelistHistory(ID blizzard.ItemID) (priceListHistory, error) {
	return priceListHistory{}, nil
}

func (phdBase pricelistHistoryDatabase) persistPricelists(targetDate time.Time, pList priceList) error {
	logging.WithFields(logrus.Fields{
		"target_date": targetDate.Unix(),
		"pricelists":  len(pList),
	}).Debug("Writing pricelists")

	err := phdBase.db.Batch(func(tx *bolt.Tx) error {
		if true {
			return errors.New("NYI")
		}

		for ID, pricesValue := range pList {
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
