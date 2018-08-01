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

func (iRequest infoRequest) resolve(sta state) infoResponse {
	regValuations := map[regionName]regionValuation{}
	for _, reg := range sta.regions {
		regValuation := regionValuation{reg, map[blizzard.RealmSlug]realmValuation{}}
		for _, rea := range sta.statuses[reg.Name].Realms {
			aucs := sta.auctions[reg.Name][rea.Slug]

			totalQuantity := int64(0)
			totalBuyout := int64(0)
			totalAuctions := 0
			owners := map[ownerName]struct{}{}
			for _, auc := range aucs {
				totalQuantity += auc.Quantity * int64(len(auc.AucList))
				totalBuyout += auc.Quantity * auc.Buyout
				totalAuctions += len(auc.AucList)
				owners[auc.Owner] = struct{}{}
			}

			reaValuation := realmValuation{
				Realm:         rea,
				TotalQuantity: totalQuantity,
				TotalBuyout:   totalBuyout,
				TotalSellers:  len(owners),
				TotalAuctions: totalAuctions,
			}

			regValuation.RealmValuations[rea.Slug] = reaValuation
		}

		regValuations[reg.Name] = regValuation
	}

	return infoResponse{
		ItemCount:        len(sta.items),
		RegionValuations: regValuations,
	}
}

func (sta state) listenForInfo(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.Status, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		iRequest, err := newInfoRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		iResponse := iRequest.resolve(sta)

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
