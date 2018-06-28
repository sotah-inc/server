package main

import (
	"encoding/json"
	"errors"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
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

	return string(jsonEncodedMessage), nil
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

		pList := newPriceList(realmAuctions)

		plResponse := priceListResponse{pList}
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

func newPriceList(maList miniAuctionList) priceList {
	pList := map[blizzard.ItemID]prices{}

	return pList
}

type priceList map[blizzard.ItemID]prices

type prices struct {
	Bid    int64 `json:"bid"`
	Buyout int64 `json:"buyout"`
}
