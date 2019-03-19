package database

import (
	"time"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

type regionRealmDatabaseShards map[blizzard.RegionName]realmDatabaseShards

type realmDatabaseShards map[blizzard.RealmSlug]PricelistHistoryDatabaseShards

type PricelistHistoryDatabaseShards map[sotah.UnixTimestamp]PricelistHistoryDatabase

func (phdShards PricelistHistoryDatabaseShards) GetPriceHistory(
	rea sotah.Realm,
	ItemId blizzard.ItemID,
	lowerBounds time.Time,
	upperBounds time.Time,
) (sotah.PriceHistory, error) {
	pHistory := sotah.PriceHistory{}

	for _, phdBase := range phdShards {
		receivedHistory, err := phdBase.getItemPriceHistory(ItemId)
		if err != nil {
			return sotah.PriceHistory{}, err
		}

		for targetTimestamp, pricesValue := range receivedHistory {
			if int64(targetTimestamp) < lowerBounds.Unix() {
				continue
			}
			if int64(targetTimestamp) > upperBounds.Unix() {
				continue
			}

			pHistory[targetTimestamp] = pricesValue
		}
	}

	return pHistory, nil
}
