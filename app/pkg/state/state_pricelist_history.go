package state

import (
	"encoding/base64"
	"encoding/json"
	"time"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/logging"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/util"
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
	RegionName  regionName         `json:"region_name"`
	RealmSlug   blizzard.RealmSlug `json:"realm_slug"`
	ItemIds     []blizzard.ItemID  `json:"item_ids"`
	LowerBounds int64              `json:"lower_bounds"`
	UpperBounds int64              `json:"upper_bounds"`
}

func (plhRequest priceListHistoryRequest) resolve(sta State) (realm, pricelistHistoryDatabaseShards, requestError) {
	regionStatuses, ok := sta.Statuses[plhRequest.RegionName]
	if !ok {
		return realm{}, pricelistHistoryDatabaseShards{}, requestError{codes.NotFound, "Invalid region (Statuses)"}
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
		return realm{}, pricelistHistoryDatabaseShards{}, requestError{codes.NotFound, "Invalid Realm (Statuses)"}
	}

	phdShards, reErr := func() (pricelistHistoryDatabaseShards, requestError) {
		regionShards, ok := sta.PricelistHistoryDatabases[plhRequest.RegionName]
		if !ok {
			return pricelistHistoryDatabaseShards{}, requestError{codes.NotFound, "Invalid region (pricelist-history databases)"}
		}

		realmShards, ok := regionShards[plhRequest.RealmSlug]
		if !ok {
			return pricelistHistoryDatabaseShards{}, requestError{codes.NotFound, "Invalid Realm (pricelist-histories)"}
		}

		return realmShards, requestError{codes.Ok, ""}
	}()

	return *rea, phdShards, reErr
}

func (sta State) listenForPriceListHistory(stop ListenStopChan) error {
	err := sta.Messenger.subscribe(subjects.PriceListHistory, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		// resolving the request
		plhRequest, err := newPriceListHistoryRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		// resolving the database from the request
		rea, tdMap, reErr := plhRequest.resolve(sta)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		logging.WithField("database-shards", len(tdMap)).Info("Querying database shards")

		// gathering up pricelist history
		plhResponse := priceListHistoryResponse{History: map[blizzard.ItemID]priceListHistory{}}
		for _, ID := range plhRequest.ItemIds {
			plHistory, err := tdMap.getPricelistHistory(
				rea,
				ID,
				time.Unix(plhRequest.LowerBounds, 0),
				time.Unix(plhRequest.UpperBounds, 0),
			)
			if err != nil {
				m.Err = err.Error()
				m.Code = codes.GenericError
				sta.Messenger.replyTo(natsMsg, m)

				return
			}

			plhResponse.History[ID] = plHistory
		}

		// encoding the message for the response
		data, err := plhResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = data
		sta.Messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
