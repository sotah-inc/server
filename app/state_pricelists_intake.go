package main

import (
	"encoding/json"

	"github.com/ihsw/sotah-server/app/blizzard"

	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
)

func newPricelistsIntakeRequest(payload []byte) (pricelistsIntakeRequest, error) {
	piRequest := &pricelistsIntakeRequest{}
	err := json.Unmarshal(payload, &piRequest)
	if err != nil {
		return pricelistsIntakeRequest{}, err
	}

	return *piRequest, nil
}

type pricelistsIntakeRequest struct {
	RegionResults map[regionName]map[blizzard.RealmSlug]int64 `json:"results"`
}

func (sta state) listenForPricelistsIntake(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.PricelistsIntake, stop, func(natsMsg nats.Msg) {
		// // writing pricelists to db
		// lastModifiedKey := make([]byte, 8)
		// binary.LittleEndian.PutUint64(lastModifiedKey, uint64(job.lastModified.Unix()))
		// pLists := newPriceList(itemIds, minimizedAuctions)
		// db := sta.databases[reg.Name][rea.Slug].db
		// log.WithFields(log.Fields{
		// 	"region": reg.Name,
		// 	"realm":  rea.Slug,
		// 	"count":  len(pLists),
		// }).Info("Writing pricelists")
		// err := db.Batch(func(tx *bolt.Tx) error {
		// 	for itemID, pList := range pLists {
		// 		b, err := tx.CreateBucketIfNotExists([]byte(fmt.Sprintf("item-prices/%d", itemID)))
		// 		if err != nil {
		// 			return err
		// 		}

		// 		result, err := json.Marshal(pList)
		// 		if err != nil {
		// 			return err
		// 		}

		// 		if err = b.Put(lastModifiedKey, result); err != nil {
		// 			return err
		// 		}
		// 	}

		// 	return nil
		// })
		// if err != nil {
		// 	return auctionsIntakeResult{}, err
		// }
	})
	if err != nil {
		return err
	}

	return nil
}
