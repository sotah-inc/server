package state

import (
	"encoding/json"
	"time"

	"github.com/sotah-inc/server/app/pkg/sotah"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func newPriceListHistoryV2Request(payload []byte) (priceListHistoryV2Request, error) {
	plhRequest := &priceListHistoryV2Request{}
	err := json.Unmarshal(payload, &plhRequest)
	if err != nil {
		return priceListHistoryV2Request{}, err
	}

	return *plhRequest, nil
}

type priceListHistoryV2Request struct {
	RegionName  blizzard.RegionName `json:"region_name"`
	RealmSlug   blizzard.RealmSlug  `json:"realm_slug"`
	ItemIds     []blizzard.ItemID   `json:"item_ids"`
	LowerBounds int64               `json:"lower_bounds"`
	UpperBounds int64               `json:"upper_bounds"`
}

func (plhRequest priceListHistoryV2Request) resolve(pubState PubState) (sotah.Realm, database.PricelistHistoryDatabaseV2Shards, requestError) {
	regionStatuses, ok := pubState.Statuses[plhRequest.RegionName]
	if !ok {
		return sotah.Realm{},
			database.PricelistHistoryDatabaseV2Shards{},
			requestError{codes.NotFound, "Invalid region (statuses)"}
	}
	rea := func() *sotah.Realm {
		for _, regionRealm := range regionStatuses.Realms {
			if regionRealm.Slug == plhRequest.RealmSlug {
				return &regionRealm
			}
		}

		return nil
	}()
	if rea == nil {
		return sotah.Realm{},
			database.PricelistHistoryDatabaseV2Shards{},
			requestError{codes.NotFound, "Invalid realm (statuses)"}
	}

	phdShards, reErr := func() (database.PricelistHistoryDatabaseV2Shards, requestError) {
		regionShards, ok := pubState.IO.Databases.PricelistHistoryDatabasesV2.Databases[plhRequest.RegionName]
		if !ok {
			return database.PricelistHistoryDatabaseV2Shards{}, requestError{codes.NotFound, "Invalid region (pricelist-history databases)"}
		}

		realmShards, ok := regionShards[plhRequest.RealmSlug]
		if !ok {
			return database.PricelistHistoryDatabaseV2Shards{}, requestError{codes.NotFound, "Invalid realm (pricelist-histories)"}
		}

		return realmShards, requestError{codes.Ok, ""}
	}()

	return *rea, phdShards, reErr
}

func (pubState PubState) ListenForPriceListHistory(stop ListenStopChan) error {
	err := pubState.IO.Messenger.Subscribe(string(subjects.PriceListHistoryV2), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		plhRequest, err := newPriceListHistoryV2Request(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			pubState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// resolving the database from the request
		rea, phdShards, reErr := plhRequest.resolve(pubState)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			pubState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		logging.WithField("database-shards", len(phdShards)).Info("Querying database shards")

		// gathering up pricelist history
		plhResponse := priceListHistoryResponse{History: sotah.ItemPriceHistories{}}
		for _, ID := range plhRequest.ItemIds {
			plHistory, err := phdShards.GetPriceHistory(
				rea,
				ID,
				time.Unix(plhRequest.LowerBounds, 0),
				time.Unix(plhRequest.UpperBounds, 0),
			)
			if err != nil {
				m.Err = err.Error()
				m.Code = codes.GenericError
				pubState.IO.Messenger.ReplyTo(natsMsg, m)

				return
			}

			plhResponse.History[ID] = plHistory
		}

		// encoding the message for the response
		data, err := plhResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			pubState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = data
		pubState.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
