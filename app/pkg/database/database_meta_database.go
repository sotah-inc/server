package database

import (
	"errors"

	"github.com/boltdb/bolt"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func NewMetaDatabase(dbFilepath string) (MetaDatabase, error) {
	db, err := bolt.Open(dbFilepath, 0600, nil)
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

func (d MetaDatabase) SetPricelistHistoriesVersion(
	regionName blizzard.RegionName,
	realmSlug blizzard.RealmSlug,
	targetTimestamp sotah.UnixTimestamp,
	versionId string,
) error {
	err := d.db.Batch(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(metaBucketName(regionName, realmSlug))
		if err != nil {
			return err
		}

		return bkt.Put(metaPricelistHistoryVersionKeyName(targetTimestamp), []byte(versionId))
	})
	if err != nil {
		return err
	}

	return nil
}
