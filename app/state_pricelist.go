package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
	"github.com/nats-io/go-nats"
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
	RegionName regionName         `json:"region_name"`
	RealmSlug  blizzard.RealmSlug `json:"realm_slug"`
	ItemIds    []blizzard.ItemID  `json:"item_ids"`
}

func (plRequest priceListRequest) resolve(sta state) (miniAuctionList, requestError) {
	regionAuctions, ok := sta.auctions[plRequest.RegionName]
	if !ok {
		return miniAuctionList{}, requestError{codes.NotFound, "Invalid region"}
	}

	realmAuctions, ok := regionAuctions[plRequest.RealmSlug]
	if !ok {
		return miniAuctionList{}, requestError{codes.NotFound, "Invalid realm"}
	}

	result := make(miniAuctionList, len(realmAuctions))
	copy(result, realmAuctions)

	return result, requestError{codes.Ok, ""}
}

func newPriceListResponseFromMessenger(plRequest priceListRequest, mess messenger) (priceListResponse, error) {
	encodedMessage, err := json.Marshal(plRequest)
	if err != nil {
		return priceListResponse{}, err
	}

	msg, err := mess.request(subjects.PriceList, encodedMessage)
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
	PriceList priceList `json:"price_list"`
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

func (sta state) listenForPriceList(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.PriceList, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		// resolving the request
		plRequest, err := newPriceListRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// resolving data from state
		realmAuctions, reErr := plRequest.resolve(sta)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// deriving a pricelist-response from the provided realm auctions
		plResponse := priceListResponse{newPriceList(plRequest.ItemIds, realmAuctions)}
		data, err := plResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = data
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func newPriceList(itemIds []blizzard.ItemID, maList miniAuctionList) priceList {
	pList := map[blizzard.ItemID]prices{}

	itemIDMap := make(map[blizzard.ItemID]struct{}, len(itemIds))
	for _, id := range itemIds {
		itemIDMap[id] = struct{}{}
	}

	for _, mAuction := range maList {
		id := mAuction.Item.ID

		if _, ok := itemIDMap[id]; !ok {
			continue
		}

		p, ok := pList[id]
		if !ok {
			p = prices{0, 0, 0}
		}

		auctionBid := float64(mAuction.Bid / mAuction.Quantity)
		if p.Bid == 0 || auctionBid < p.Bid {
			p.Bid = auctionBid
		}

		auctionBuyout := float64(mAuction.Buyout / mAuction.Quantity)
		if p.Buyout == 0 || auctionBuyout < p.Buyout {
			p.Buyout = auctionBuyout
		}

		p.Volume += mAuction.Quantity * int64(len(mAuction.AucList))

		pList[id] = p
	}

	return pList
}

type priceList map[blizzard.ItemID]prices

type prices struct {
	Bid    float64 `json:"bid"`
	Buyout float64 `json:"buyout"`
	Volume int64   `json:"volume"`
}
