package database

import (
	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func newLiveAuctionsDatabase(dirPath string, rea sotah.Realm) (liveAuctionsDatabase, error) {
	dbFilepath := liveAuctionsDatabasePath(dirPath, rea)
	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return liveAuctionsDatabase{}, err
	}

	return liveAuctionsDatabase{db, rea}, nil
}

type liveAuctionsDatabase struct {
	db    *bolt.DB
	realm sotah.Realm
}

func (ladBase liveAuctionsDatabase) persistMiniAuctionList(maList sotah.MiniAuctionList) error {
	logging.WithFields(logrus.Fields{
		"db":                 ladBase.db.Path(),
		"mini-auctions-list": len(maList),
	}).Debug("Persisting mini-auction-list")

	encodedData, err := maList.EncodeForDatabase()
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

func (ladBase liveAuctionsDatabase) persistEncodedData(encodedData []byte) error {
	logging.WithFields(logrus.Fields{
		"db":           ladBase.db.Path(),
		"encoded-data": len(encodedData),
	}).Debug("Persisting mini-auction-list via encoded-data")

	err := ladBase.db.Update(func(tx *bolt.Tx) error {
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

func (ladBase liveAuctionsDatabase) GetMiniAuctionList() (sotah.MiniAuctionList, error) {
	out := sotah.MiniAuctionList{}

	err := ladBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(liveAuctionsBucketName())
		if bkt == nil {
			logging.WithFields(logrus.Fields{
				"db":          ladBase.db.Path(),
				"bucket-name": string(liveAuctionsBucketName()),
			}).Error("Live-auctions bucket not found")

			return nil
		}

		var err error
		out, err = sotah.NewMiniAuctionListFromGzipped(bkt.Get(liveAuctionsKeyName()))
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return sotah.MiniAuctionList{}, err
	}

	return out, nil
}

type miniAuctionListStats struct {
	TotalAuctions int
	TotalQuantity int
	TotalBuyout   int
	OwnerNames    []sotah.OwnerName
	ItemIds       []blizzard.ItemID
	AuctionIds    []int64
}

func (ladBase liveAuctionsDatabase) stats() (miniAuctionListStats, error) {
	maList, err := ladBase.GetMiniAuctionList()
	if err != nil {
		return miniAuctionListStats{}, err
	}

	out := miniAuctionListStats{
		TotalAuctions: maList.TotalAuctions(),
		TotalQuantity: maList.TotalQuantity(),
		TotalBuyout:   int(maList.TotalBuyout()),
		OwnerNames:    maList.OwnerNames(),
		ItemIds:       maList.ItemIds(),
		AuctionIds:    maList.AuctionIds(),
	}

	return out, nil
}
