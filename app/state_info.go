package main

import (
	"encoding/json"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
)

type realmValuation struct {
	Realm         realm `json:"realm"`
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

func (iRequest infoRequest) resolve(sta state) (infoResponse, error) {
	regValuations := map[regionName]regionValuation{}
	for _, reg := range sta.resolver.config.filterInRegions(sta.regions) {
		regValuation := regionValuation{reg, map[blizzard.RealmSlug]realmValuation{}}
		for _, rea := range sta.resolver.config.filterInRealms(reg, sta.statuses[reg.Name].Realms) {
			maList, err := sta.liveAuctionsDatabases[reg.Name][rea.Slug].getMiniauctions()
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

	items, err := sta.itemsDatabase.getItems()
	if err != nil {
		return infoResponse{}, err
	}

	return infoResponse{len(items), regValuations}, nil
}

func (sta state) listenForInfo(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.Info, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		iRequest, err := newInfoRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		iResponse, err := iRequest.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		encodedStatus, err := json.Marshal(iResponse)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedStatus)
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
