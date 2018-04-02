package main

import (
	"encoding/json"
	"fmt"

	"github.com/ihsw/sotah-server/app/codes"

	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
)

type state struct {
	messenger messenger
	resolver  resolver

	regions  []region
	statuses map[regionName]*status
	auctions map[regionName]map[realmSlug]*auctions
}

type listenForStatusMessage struct {
	RegionName regionName `json:"region_name"`
}

func (sta state) listenForStatus(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Status, stop, func(natsMsg *nats.Msg) {
		m := newMessage()

		lm := &listenForStatusMessage{}
		err := json.Unmarshal(natsMsg.Data, &lm)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		var region region
		for _, r := range sta.regions {
			if r.Name != lm.RegionName {
				continue
			}

			region = r
			break
		}

		regionStatus, ok := sta.statuses[lm.RegionName]
		if !ok {
			regionStatus, err = region.getStatus(sta.resolver)
			if err != nil {
				m.Err = fmt.Sprintf("Could not fetch region: %s", err.Error())
				m.Code = codes.GenericError
				sta.messenger.replyTo(natsMsg, m)

				return
			}

			sta.statuses[region.Name] = regionStatus
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

type listenForAuctionsMessage struct {
	RegionName regionName `json:"region_name"`
	RealmSlug  realmSlug  `json:"realm_slug"`
}

func (sta state) listenForAuctions(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Auctions, stop, func(natsMsg *nats.Msg) {
		m := newMessage()

		am := &listenForAuctionsMessage{}
		err := json.Unmarshal(natsMsg.Data, &am)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		aList, ok := sta.auctions[am.RegionName]
		if !ok {
			m.Err = "Invalid region"
			m.Code = codes.NotFound
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		a, ok := aList[am.RealmSlug]
		if !ok {
			m.Err = "Invalid realm"
			m.Code = codes.NotFound
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		encodedStatus, err := json.Marshal(a)
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
