package main

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/ihsw/sotah-server/app/blizzard"

	"github.com/boltdb/bolt"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/sirupsen/logrus"
)

func liveAuctionsBucketName() []byte {
	return []byte("live-auctions")
}

func liveAuctionsKeyName() []byte {
	return []byte("live-auctions")
}

func liveAuctionsDatabasePath(c config, reg region, rea realm) (string, error) {
	return filepath.Abs(
		fmt.Sprintf("%s/databases/%s/%s/live-auctions.db", c.CacheDir, reg.Name, rea.Slug),
	)
}

func newLiveAuctionsDatabase(c config, reg region, rea realm) (liveAuctionsDatabase, error) {
	dbFilepath, err := liveAuctionsDatabasePath(c, reg, rea)
	if err != nil {
		return liveAuctionsDatabase{}, err
	}

	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return liveAuctionsDatabase{}, err
	}

	return liveAuctionsDatabase{db, rea}, nil
}

type liveAuctionsDatabase struct {
	db    *bolt.DB
	realm realm
}

func (ladBase liveAuctionsDatabase) persistMiniauctions(maList miniAuctionList) error {
	logging.WithFields(logrus.Fields{
		"region":            ladBase.realm.region.Name,
		"realm":             ladBase.realm.Slug,
		"miniauctions-list": len(maList),
	}).Debug("Persisting miniauctions-list")

	encodedData, err := maList.encodeForDatabase()
	if err != nil {
		return err
	}

	err = ladBase.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(liveAuctionsBucketName())
		if err != nil {
			return err
		}

		if err := bkt.Put(liveAuctionsKeyName(), encodedData); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (ladBase liveAuctionsDatabase) getMiniauctions() (miniAuctionList, error) {
	out := miniAuctionList{}
	err := ladBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(liveAuctionsBucketName())
		if bkt == nil {
			logging.WithFields(logrus.Fields{
				"region":      ladBase.realm.region.Name,
				"realm":       ladBase.realm.Slug,
				"bucket-name": string(liveAuctionsBucketName()),
			}).Error("Live-auctions bucket not found")

			return errors.New("Bucket not found")
		}

		var err error
		out, err = newMiniAuctionsListFromGzipped(bkt.Get(liveAuctionsKeyName()))
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return miniAuctionList{}, err
	}

	return out, nil
}

func newLiveAuctionsDatabases(c config, regs regionList, stas statuses) (liveAuctionsDatabases, error) {
	ladBases := liveAuctionsDatabases{}

	for _, reg := range c.filterInRegions(regs) {
		ladBases[reg.Name] = map[blizzard.RealmSlug]liveAuctionsDatabase{}

		for _, rea := range c.filterInRealms(reg, stas[reg.Name].Realms) {
			ladBase, err := newLiveAuctionsDatabase(c, reg, rea)
			if err != nil {
				return liveAuctionsDatabases{}, err
			}

			ladBases[reg.Name][rea.Slug] = ladBase
		}
	}

	return ladBases, nil
}

type liveAuctionsDatabases map[regionName]map[blizzard.RealmSlug]liveAuctionsDatabase
