package main

import (
	"encoding/base64"
	"encoding/json"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
	"github.com/nats-io/go-nats"
)

type priceListHistoryResponse struct {
	History map[blizzard.ItemID]priceListHistory `json:"history"`
}

func (plhResponse priceListHistoryResponse) encodeForMessage() (string, error) {
	jsonEncodedResponse, err := json.Marshal(plhResponse)
	if err != nil {
		return "", err
	}

	gzipEncodedResponse, err := util.GzipEncode(jsonEncodedResponse)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(gzipEncodedResponse), nil
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

func (plhRequest priceListHistoryRequest) resolve(sta state) (realm, timestampDatabaseMap, requestError) {
	regionStatuses, ok := sta.statuses[plhRequest.RegionName]
	if !ok {
		return realm{}, timestampDatabaseMap{}, requestError{codes.NotFound, "Invalid region"}
	}
	rea := func() *realm {
		for _, regionRealm := range regionStatuses.Realms {
			if regionRealm.Slug == plhRequest.RealmSlug {
				return &regionRealm
			}
		}

		return nil
	}()
	if rea == nil {
		return realm{}, timestampDatabaseMap{}, requestError{codes.NotFound, "Invalid realm"}
	}

	tdMap, ok := sta.databases[plhRequest.RegionName][plhRequest.RealmSlug]
	if !ok {
		return realm{}, timestampDatabaseMap{}, requestError{codes.NotFound, "Invalid region"}
	}

	return *rea, tdMap, requestError{codes.Ok, ""}
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
		rea, tdMap, reErr := plhRequest.resolve(sta)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// gathering up pricelist history
		plhResponse := priceListHistoryResponse{History: map[blizzard.ItemID]priceListHistory{}}
		for _, ID := range plhRequest.ItemIds {
			for _, dBase := range tdMap {
				// gathering history from the database shard
				receivedHistory, err := dBase.getPricelistHistory(rea, ID)
				if err != nil {
					m.Err = err.Error()
					m.Code = codes.GenericError
					sta.messenger.replyTo(natsMsg, m)

					return
				}

				// resolving the item's history in the response
				history := func() priceListHistory {
					history, ok := plhResponse.History[ID]
					if !ok {
						return priceListHistory{}
					}

					return history
				}()

				// appending the shard-specific history to the aggregated history
				for unixTimestamp, pricesValue := range receivedHistory {
					history[unixTimestamp] = pricesValue
				}

				// writing the history out to the response
				plhResponse.History[ID] = history
			}
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
