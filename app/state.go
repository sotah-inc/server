package main

import (
	"encoding/json"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"
)

type requestError struct {
	code    codes.Code
	message string
}

type state struct {
	messenger messenger
	resolver  resolver
	listeners listeners

	regions     []region
	statuses    map[regionName]status
	auctions    map[regionName]map[blizzard.RealmSlug]miniAuctionList
	items       itemsMap
	itemClasses blizzard.ItemClasses
}

func (sta state) listenForRegions(stop listenStopChan) error {
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

func (sta state) listenForGenericTestErrors(stop listenStopChan) error {
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

	// compacting the auctions
	minimizedAuctions := newMiniAuctionListFromBlizzardAuctions(job.auctions.Auctions)

	// loading the minimized auctions into state
	sta.auctions[reg.Name][rea.Slug] = minimizedAuctions

	// setting the realm last-modified
	for i, statusRealm := range sta.statuses[reg.Name].Realms {
		if statusRealm.Slug != rea.Slug {
			continue
		}

		sta.statuses[reg.Name].Realms[i].LastModified = job.lastModified.Unix()

		break
	}

	// returning a list of item ids for syncing
	return minimizedAuctions.itemIds()
}

type listenStopChan chan interface{}

type listenFunc func(stop listenStopChan) error

type subjectListeners map[subjects.Subject]listenFunc

func newListeners(sListeners subjectListeners) listeners {
	ls := listeners{}
	for subj, l := range sListeners {
		ls[subj] = listener{l, make(listenStopChan)}
	}

	return ls
}

type listeners map[subjects.Subject]listener

func (ls listeners) listen() error {
	log.WithField("listeners", len(ls)).Info("Starting listeners")

	for _, l := range ls {
		if err := l.call(l.stopChan); err != nil {
			return err
		}
	}

	return nil
}

func (ls listeners) stop() {
	log.Info("Stopping listeners")

	for _, l := range ls {
		l.stopChan <- struct{}{}
	}
}

type listener struct {
	call     listenFunc
	stopChan listenStopChan
}

type workerStopChan chan interface{}
