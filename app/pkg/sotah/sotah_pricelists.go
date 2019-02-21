package sotah

import (
	"encoding/json"
	"github.com/sotah-inc/server/app/pkg/logging"
	"math"
	"sort"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/util"
)

// item-prices
func NewItemPrices(maList MiniAuctionList) ItemPrices {
	itemIds := maList.ItemIds()
	iPrices := map[blizzard.ItemID]Prices{}
	itemIDMap := make(map[blizzard.ItemID]struct{}, len(itemIds))
	itemBuyoutPers := make(map[blizzard.ItemID][]float64, len(itemIds))
	for _, id := range itemIds {
		iPrices[id] = Prices{}
		itemIDMap[id] = struct{}{}
		itemBuyoutPers[id] = []float64{}
	}

	for _, mAuction := range maList {
		id := mAuction.ItemID

		if _, ok := itemIDMap[id]; !ok {
			continue
		}

		p := iPrices[id]

		if mAuction.Buyout > 0 {
			auctionBuyoutPer := float64(mAuction.Buyout / mAuction.Quantity)

			itemBuyoutPers[id] = append(itemBuyoutPers[id], auctionBuyoutPer)

			if p.MinBuyoutPer == 0 || auctionBuyoutPer < p.MinBuyoutPer {
				p.MinBuyoutPer = auctionBuyoutPer
			}
			if p.MaxBuyoutPer == 0 || auctionBuyoutPer > p.MaxBuyoutPer {
				p.MaxBuyoutPer = auctionBuyoutPer
			}
		}

		p.Volume += mAuction.Quantity * int64(len(mAuction.AucList))

		iPrices[id] = p
	}

	for id, buyouts := range itemBuyoutPers {
		if len(buyouts) == 0 {
			continue
		}

		p := iPrices[id]

		// gathering total and calculating average
		total := float64(0)
		for _, buyout := range buyouts {
			total += buyout
		}
		p.AverageBuyoutPer = total / float64(len(buyouts))

		// sorting buyouts and calculating median
		buyoutsSlice := sort.Float64Slice(buyouts)
		buyoutsSlice.Sort()
		hasEvenMembers := len(buyoutsSlice)%2 == 0
		median := float64(0)
		if hasEvenMembers {
			middle := float64(len(buyoutsSlice)) / 2
			median = (buyoutsSlice[int(math.Floor(middle))] + buyoutsSlice[int(math.Ceil(middle))]) / 2
		} else {
			median = buyoutsSlice[(len(buyoutsSlice)-1)/2]
		}
		p.MedianBuyoutPer = median

		iPrices[id] = p
	}

	return iPrices
}

type ItemPrices map[blizzard.ItemID]Prices

func (iPrices ItemPrices) ItemIds() []blizzard.ItemID {
	out := []blizzard.ItemID{}
	for ID := range iPrices {
		out = append(out, ID)
	}

	return out
}

// prices
func NewPricesFromBytes(data []byte) (Prices, error) {
	gzipDecoded, err := util.GzipDecode(data)
	if err != nil {
		return Prices{}, err
	}

	pricesValue := Prices{}
	if err := json.Unmarshal(gzipDecoded, &pricesValue); err != nil {
		return Prices{}, err
	}

	return pricesValue, nil
}

type Prices struct {
	MinBuyoutPer     float64 `json:"min_buyout_per"`
	MaxBuyoutPer     float64 `json:"max_buyout_per"`
	AverageBuyoutPer float64 `json:"average_buyout_per"`
	MedianBuyoutPer  float64 `json:"median_buyout_per"`
	Volume           int64   `json:"volume"`
}

func (p Prices) EncodeForPersistence() ([]byte, error) {
	jsonEncoded, err := json.Marshal(p)
	if err != nil {
		return []byte{}, err
	}

	gzipEncoded, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return []byte{}, err
	}

	return gzipEncoded, nil
}

// item-price-histories
func NewItemPriceHistoriesFromGzipped(data []byte) (ItemPriceHistories, error) {
	logging.WithField("data", len(data)).Info("Gzip-decoding data")

	gzipDecoded, err := util.GzipDecode(data)
	if err != nil {
		return ItemPriceHistories{}, err
	}

	logging.WithField("data", len(gzipDecoded)).Info("Json-decoding data")

	out := ItemPriceHistories{}
	if err := json.Unmarshal(gzipDecoded, &out); err != nil {
		return ItemPriceHistories{}, err
	}

	logging.WithField("item-price-histories", len(out)).Info("Decoded item-price-histories from json")

	return out, nil
}

type ItemPriceHistories map[blizzard.ItemID]PriceHistory

func (ipHistories ItemPriceHistories) EncodeForPersistence() ([]byte, error) {
	jsonEncoded, err := json.Marshal(ipHistories)
	if err != nil {
		return []byte{}, err
	}

	gzipEncoded, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return []byte{}, err
	}

	return gzipEncoded, nil
}

// price-history
func NewPriceHistoryFromBytes(data []byte) (PriceHistory, error) {
	gzipDecoded, err := util.GzipDecode(data)
	if err != nil {
		return PriceHistory{}, err
	}

	out := PriceHistory{}
	if err := json.Unmarshal(gzipDecoded, &out); err != nil {
		return PriceHistory{}, err
	}

	return out, nil
}

type PriceHistory map[UnixTimestamp]Prices

func (pHistory PriceHistory) EncodeForPersistence() ([]byte, error) {
	jsonEncoded, err := json.Marshal(pHistory)
	if err != nil {
		return []byte{}, err
	}

	gzipEncoded, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return []byte{}, err
	}

	return gzipEncoded, nil
}
