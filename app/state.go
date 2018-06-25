package main

import (
	"encoding/json"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"
)

type state struct {
	messenger messenger
	resolver  resolver

	regions     []region
	statuses    map[regionName]status
	auctions    map[regionName]map[blizzard.RealmSlug]miniAuctionList
	items       itemsMap
	itemClasses itemClasses
}

type requestError struct {
	code    codes.Code
	message string
}

func (sta state) listenForRegions(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Regions, stop, func(natsMsg nats.Msg) {
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
	err := sta.messenger.subscribe(subjects.GenericTestErrors, stop, func(natsMsg nats.Msg) {
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

func (sta state) auctionsIntake(job getAuctionsJob) []blizzard.ItemID {
	rea := job.realm
	reg := rea.region
	if job.err != nil {
		log.WithFields(log.Fields{
			"region": reg.Name,
			"realm":  rea.Slug,
			"error":  job.err.Error(),
		}).Info("Auction fetch failure")

		return []blizzard.ItemID{}
	}

	// compacting the auctions
	minimizedAuctions := job.auctions.Auctions.minimize()

	// loading the minimized auctions into state
	sta.auctions[reg.Name][rea.Slug] = minimizedAuctions

	// returning a list of item ids for syncing
	return minimizedAuctions.itemIds()
}
