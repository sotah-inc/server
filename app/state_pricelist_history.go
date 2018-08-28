package main

import (
	"encoding/json"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/nats-io/go-nats"
)

func newPriceListHistoryRequest(payload []byte) (priceListRequest, error) {
	pList := &priceListRequest{}
	err := json.Unmarshal(payload, &pList)
	if err != nil {
		return priceListRequest{}, err
	}

	return *pList, nil
}

type priceListHistoryRequest struct {
	RegionName regionName         `json:"region_name"`
	RealmSlug  blizzard.RealmSlug `json:"realm_slug"`
	ItemIds    []blizzard.ItemID  `json:"item_ids"`
}

func (plhRequest priceListHistoryRequest) resolve(sta state) (database, requestError) {
	regionDatabases, ok := sta.databases[plhRequest.RegionName]
	if !ok {
		return database{}, requestError{codes.NotFound, "Invalid region"}
	}

	realmDatabase, ok := regionDatabases[plhRequest.RealmSlug]
	if !ok {
		return database{}, requestError{codes.NotFound, "Invalid realm"}
	}

	return realmDatabase, requestError{codes.Ok, ""}
}

func (sta state) listenForPriceListHistory(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.PriceListHistory, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		// resolving the request
		plhRequest, err := newPriceListHistoryRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// going over this realms history
		_, reErr := plhRequest.resolve(sta)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = ""
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
