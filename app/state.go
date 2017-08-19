package main

import (
	"encoding/json"

	"github.com/ihsw/sotah-server/app/codes"

	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
)

type state struct {
	messenger messenger

	config   *config
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

		regionStatus, ok := sta.statuses[lm.RegionName]
		if !ok {
			m.Err = "Region not found"
			m.Code = codes.NotFound
			sta.messenger.replyTo(natsMsg, m)

			return
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

		encodedRegions, err := json.Marshal(sta.config.Regions)
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
