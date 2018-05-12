package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
)

type state struct {
	messenger messenger
	resolver  *resolver

	regions  []region
	statuses map[regionName]*status
	auctions map[regionName]map[realmSlug]miniAuctionList
}

type requestError struct {
	code    codes.Code
	message string
}

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

func (sta state) listenForRegions(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Regions, stop, func(natsMsg *nats.Msg) {
		m := newMessage()

		encodedRegions, err := json.Marshal(sta.regions)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedRegions)
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta state) listenForGenericTestErrors(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.GenericTestErrors, stop, func(natsMsg *nats.Msg) {
		m := newMessage()
		m.Err = "Test error"
		m.Code = codes.GenericError
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func newOwnersRequest(payload []byte) (*ownersRequest, error) {
	request := &ownersRequest{}
	err := json.Unmarshal(payload, &request)
	if err != nil {
		return nil, err
	}

	return request, nil
}

type ownersRequest struct {
	RegionName regionName `json:"region_name"`
	RealmSlug  realmSlug  `json:"realm_slug"`
	Query      string     `json:"query"`
}

func (request ownersRequest) resolve(sta state) (miniAuctionList, error) {
	regionAuctions, ok := sta.auctions[request.RegionName]
	if !ok {
		return miniAuctionList{}, errors.New("Invalid region name")
	}

	realmAuctions, ok := regionAuctions[request.RealmSlug]
	if !ok {
		return miniAuctionList{}, errors.New("Invalid realm slug")
	}

	return realmAuctions, nil
}

func (sta state) listenForOwners(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Owners, stop, func(natsMsg *nats.Msg) {
		m := newMessage()

		// resolving the request
		request, err := newOwnersRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// resolving miniauctionslist from the request and state
		mal, err := request.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.NotFound
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		o := newOwnersFromAuctions(mal)

		// optionally filtering in matches
		if request.Query != "" {
			o.Owners = o.Owners.filter(request.Query)
		}

		// sorting and truncating
		sort.Sort(ownersByName(o.Owners))
		o.Owners = o.Owners.limit()

		// marshalling for messenger
		encodedMessage, err := json.Marshal(o)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// dumping it out
		m.Data = string(encodedMessage)
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
