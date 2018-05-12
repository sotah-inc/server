package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
)

func newStatusRequest(payload []byte) (*statusRequest, error) {
	sr := &statusRequest{}
	err := json.Unmarshal(payload, &sr)
	if err != nil {
		return nil, err
	}

	return sr, nil
}

type statusRequest struct {
	RegionName regionName `json:"region_name"`
}

func (sr statusRequest) resolve(sta state) (region, error) {
	var reg region
	for _, r := range sta.regions {
		if r.Name != sr.RegionName {
			continue
		}

		reg = r
		break
	}

	if reg.Name == "" {
		return region{}, errors.New("Invalid region")
	}

	return reg, nil
}

func (sta state) listenForStatus(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Status, stop, func(natsMsg *nats.Msg) {
		m := newMessage()

		sr, err := newStatusRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		reg, err := sr.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.NotFound
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		regionStatus, ok := sta.statuses[sr.RegionName]
		if !ok {
			if sta.resolver == nil {
				m.Err = "Resolver not defined"
				m.Code = codes.GenericError
				sta.messenger.replyTo(natsMsg, m)

				return
			}

			regionStatus, err = reg.getStatus(*sta.resolver)
			if err != nil {
				m.Err = fmt.Sprintf("Could not fetch region: %s", err.Error())
				m.Code = codes.GenericError
				sta.messenger.replyTo(natsMsg, m)

				return
			}

			if regionStatus == nil {
				m.Err = "Region-status was nil"
				m.Code = codes.GenericError
				sta.messenger.replyTo(natsMsg, m)

				return
			}

			sta.statuses[reg.Name] = regionStatus
			for _, realm := range regionStatus.Realms {
				sta.auctions[reg.Name][realm.Slug] = miniAuctionList{}
			}
		}

		encodedStatus, err := json.Marshal(regionStatus)
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
