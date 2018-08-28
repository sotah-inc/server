package main

import (
	"encoding/json"

	"github.com/boltdb/bolt"
	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/nats-io/go-nats"
)

type priceListHistoryResponse struct {
	History map[int64]priceList `json:"history"`
}

func (plhResponse priceListHistoryResponse) encodeForMessage() (string, error) {
	return "", nil
}

func newPriceListHistoryRequest(payload []byte) (priceListHistoryRequest, error) {
	plhRequest := &priceListHistoryRequest{}
	err := json.Unmarshal(payload, &plhRequest)
	if err != nil {
		return priceListHistoryRequest{}, err
	}

	return *plhRequest, nil
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

		// resolving the database from the request
		realmDatabase, reErr := plhRequest.resolve(sta)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// gathering up pricelist history
		plhResponse := priceListHistoryResponse{History: map[int64]priceList{}}
		err = realmDatabase.db.View(func(tx *bolt.Tx) error {
			return nil
		})
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// encoding the message for the response
		data, err := plhResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
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
