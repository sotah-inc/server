package main

import (
	"encoding/json"

	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/nats-io/go-nats"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
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

func (plRequest priceListRequest) resolve(sta state) (priceList, requestError) {
	return priceList{}, requestError{codes.Ok, ""}
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

		plResponse := priceListResponse{}
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
	bid    int64
	buyout int64
}
