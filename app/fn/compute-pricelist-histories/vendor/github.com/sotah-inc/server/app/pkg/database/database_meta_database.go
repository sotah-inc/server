package database

import (
	"errors"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"

	"github.com/boltdb/bolt"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func NewMetaDatabase(dbDir string) (MetaDatabase, error) {
	db, err := bolt.Open(metaDatabaseFilePath(dbDir), 0600, nil)
	if err != nil {
		return MetaDatabase{}, err
	}

	return MetaDatabase{db}, nil
}

type MetaDatabase struct {
	db *bolt.DB
}

func (d MetaDatabase) HasBucket(regionName blizzard.RegionName, realmSlug blizzard.RealmSlug) (bool, error) {
	out := false
	err := d.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(metaBucketName(regionName, realmSlug))
		if bkt == nil {
			out = false

			return nil
		}

		return nil
	})
	if err != nil {
		return false, err
	}

	return out, nil
}

func (d MetaDatabase) HasPricelistHistoriesVersion(
	regionName blizzard.RegionName,
	realmSlug blizzard.RealmSlug,
	targetTimestamp sotah.UnixTimestamp,
) (bool, error) {
	out := false
	err := d.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(metaBucketName(regionName, realmSlug))
		v := bkt.Get(metaPricelistHistoryVersionKeyName(targetTimestamp))
		if v == nil {
			out = false

			return nil
		}

		out = true

		return nil
	})
	if err != nil {
		return false, err
	}

	return out, nil
}

func (d MetaDatabase) GetPricelistHistoriesVersion(
	regionName blizzard.RegionName,
	realmSlug blizzard.RealmSlug,
	targetTimestamp sotah.UnixTimestamp,
) (string, error) {
	out := ""
	err := d.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(metaBucketName(regionName, realmSlug))
		if bkt == nil {
			return errors.New("no bucket found")
		}

		out = string(bkt.Get(metaPricelistHistoryVersionKeyName(targetTimestamp)))

		return nil
	})
	if err != nil {
		return "", err
	}

	return out, nil
}

func (d MetaDatabase) SetPricelistHistoriesVersions(versions sotah.PricelistHistoryVersions) error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		for regionName, realmVersions := range versions {
			for realmSlug, timestampVersions := range realmVersions {
				bkt, err := tx.CreateBucketIfNotExists(metaBucketName(regionName, realmSlug))
				if err != nil {
					return err
				}

				for targetTimestamp, versionId := range timestampVersions {
					logging.WithFields(logrus.Fields{
						"region":           regionName,
						"realm":            realmSlug,
						"target-timestamp": targetTimestamp,
						"version-id":       versionId,
					}).Info("Setting version")

					if err := bkt.Put(metaPricelistHistoryVersionKeyName(targetTimestamp), []byte(versionId)); err != nil {
						return err
					}
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
