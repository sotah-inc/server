package main

import (
	"encoding/json"

	"github.com/ihsw/go-download/app/subjects"
	nats "github.com/nats-io/go-nats"
)

type state struct {
	messenger messenger
	status    *status
	auctions  map[regionName]map[realmSlug]*auctions
}

func (sta state) listenForStatus(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Status, stop, func(natsMsg *nats.Msg) {
		m := message{}

		encodedStatus, err := json.Marshal(sta.status)
		if err != nil {
			m.Err = err.Error()
		} else {
			m.Data = string(encodedStatus)
		}

		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

type auctionsMessage struct {
	RegionName regionName `json:"region_name"`
	RealmSlug  realmSlug  `json:"realm_slug"`
}

func (sta state) listenForAuctions(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Auctions, stop, func(natsMsg *nats.Msg) {
		m := message{}

		am := &auctionsMessage{}
		err := json.Unmarshal(natsMsg.Data, &am)
		if err != nil {
			m.Err = err.Error()
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		aList, ok := sta.auctions[am.RegionName]
		if !ok {
			m.Err = "Invalid region"
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		a, ok := aList[am.RealmSlug]
		if !ok {
			m.Err = "Invalid realm"
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		encodedStatus, err := json.Marshal(a)
		if err != nil {
			m.Err = err.Error()
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
