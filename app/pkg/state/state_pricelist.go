package state

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"sort"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
)

func newPriceListRequest(payload []byte) (priceListRequest, error) {
	pList := &priceListRequest{}
	err := json.Unmarshal(payload, &pList)
	if err != nil {
		return priceListRequest{}, err
	}

	return *pList, nil
}

type priceListRequest struct {
	RegionName internal.RegionName `json:"region_name"`
	RealmSlug  blizzard.RealmSlug  `json:"realm_slug"`
	ItemIds    []blizzard.ItemID   `json:"item_ids"`
}

func (plRequest priceListRequest) resolve(sta State) (internal.MiniAuctionList, requestError) {
	regionLadBases, ok := sta.LiveAuctionsDatabases[plRequest.RegionName]
	if !ok {
		return internal.MiniAuctionList{}, requestError{codes.NotFound, "Invalid region"}
	}

	ladBase, ok := regionLadBases[plRequest.RealmSlug]
	if !ok {
		return internal.MiniAuctionList{}, requestError{codes.NotFound, "Invalid Realm"}
	}

	maList, err := ladBase.GetMiniauctions()
	if err != nil {
		return internal.MiniAuctionList{}, requestError{codes.GenericError, err.Error()}
	}

	return maList, requestError{codes.Ok, ""}
}

func newPriceListResponseFromMessenger(plRequest priceListRequest, mess messenger.Messenger) (priceListResponse, error) {
	encodedMessage, err := json.Marshal(plRequest)
	if err != nil {
		return priceListResponse{}, err
	}

	msg, err := mess.Request(subjects.PriceList, encodedMessage)
	if err != nil {
		return priceListResponse{}, err
	}

	if msg.Code != codes.Ok {
		return priceListResponse{}, errors.New(msg.Err)
	}

	return newPriceListResponse([]byte(msg.Data))
}

func newPriceListResponse(body []byte) (priceListResponse, error) {
	plResponse := &priceListResponse{}
	if err := json.Unmarshal(body, &plResponse); err != nil {
		return priceListResponse{}, err
	}

	return *plResponse, nil
}

type priceListResponse struct {
	PriceList PriceList `json:"price_list"`
}

func (plResponse priceListResponse) encodeForMessage() (string, error) {
	jsonEncodedMessage, err := json.Marshal(plResponse)
	if err != nil {
		return "", err
	}

	gzipEncodedMessage, err := util.GzipEncode(jsonEncodedMessage)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(gzipEncodedMessage), nil
}

func (sta State) ListenForPriceList(stop messenger.ListenStopChan) error {
	err := sta.IO.messenger.Subscribe(subjects.PriceList, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		plRequest, err := newPriceListRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		// resolving data from State
		realmAuctions, reErr := plRequest.resolve(sta)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		// deriving a pricelist-response from the provided Realm auctions
		plResponse := priceListResponse{NewPriceList(plRequest.ItemIds, realmAuctions)}
		data, err := plResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = data
		sta.IO.messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func NewPriceList(itemIds []blizzard.ItemID, maList internal.MiniAuctionList) PriceList {
	pList := map[blizzard.ItemID]Prices{}
	itemIDMap := make(map[blizzard.ItemID]struct{}, len(itemIds))
	itemBuyoutPers := make(map[blizzard.ItemID][]float64, len(itemIds))
	for _, id := range itemIds {
		pList[id] = Prices{}
		itemIDMap[id] = struct{}{}
		itemBuyoutPers[id] = []float64{}
	}

	for _, mAuction := range maList {
		id := mAuction.ItemID

		if _, ok := itemIDMap[id]; !ok {
			continue
		}

		p := pList[id]

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

		pList[id] = p
	}

	for id, buyouts := range itemBuyoutPers {
		if len(buyouts) == 0 {
			continue
		}

		p := pList[id]

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

		pList[id] = p
	}

	return pList
}

type PriceList map[blizzard.ItemID]Prices

func (pList PriceList) ItemIds() []blizzard.ItemID {
	out := []blizzard.ItemID{}
	for ID := range pList {
		out = append(out, ID)
	}

	return out
}

func newPricesFromBytes(data []byte) (Prices, error) {
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

func (p Prices) encodeForPersistence() ([]byte, error) {
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
