package state

import (
	"encoding/json"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
)

type realmValuation struct {
	Realm         realm `json:"Realm"`
	TotalQuantity int64 `json:"total_quantity"`
	TotalBuyout   int64 `json:"total_buyout"`
	TotalSellers  int   `json:"total_sellers"`
	TotalAuctions int   `json:"total_auctions"`
}

type regionValuation struct {
	Region          region                                `json:"region"`
	RealmValuations map[blizzard.RealmSlug]realmValuation `json:"realm_valuations"`
}

type infoResponse struct {
	ItemCount        int                            `json:"item_count"`
	RegionValuations map[regionName]regionValuation `json:"region_valuations"`
}

func newInfoRequest(payload []byte) (infoRequest, error) {
	iRequest := &infoRequest{}
	err := json.Unmarshal(payload, &iRequest)
	if err != nil {
		return infoRequest{}, err
	}

	return *iRequest, nil
}

type infoRequest struct{}

func (iRequest infoRequest) resolve(sta State) (infoResponse, error) {
	regValuations := map[regionName]regionValuation{}
	for _, reg := range sta.Regions {
		regValuation := regionValuation{reg, map[blizzard.RealmSlug]realmValuation{}}
		for _, rea := range sta.Statuses[reg.Name].Realms {
			maList, err := sta.LiveAuctionsDatabases[reg.Name][rea.Slug].getMiniauctions()
			if err != nil {
				return infoResponse{}, err
			}

			reaValuation := realmValuation{
				Realm:         rea,
				TotalQuantity: int64(maList.totalQuantity()),
				TotalBuyout:   maList.totalBuyout(),
				TotalSellers:  len(maList.ownerNames()),
				TotalAuctions: maList.totalAuctions(),
			}

			regValuation.RealmValuations[rea.Slug] = reaValuation
		}

		regValuations[reg.Name] = regValuation
	}

	items, err := sta.ItemsDatabase.getItems()
	if err != nil {
		return infoResponse{}, err
	}

	return infoResponse{len(items), regValuations}, nil
}

func (sta State) listenForInfo(stop messenger.ListenStopChan) error {
	err := sta.IO.messenger.Subscribe(subjects.Info, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		iRequest, err := newInfoRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		iResponse, err := iRequest.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		encodedStatus, err := json.Marshal(iResponse)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedStatus)
		sta.IO.messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
