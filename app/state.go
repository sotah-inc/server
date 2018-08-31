package main

import (
	"encoding/json"
	"time"

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

func newState(mess messenger, res resolver) state {
	return state{
		messenger:             mess,
		resolver:              res,
		regions:               res.config.Regions,
		statuses:              statuses{},
		auctionIntakeStatuses: map[regionName]map[blizzard.RealmSlug]time.Time{},
		auctions:              map[regionName]map[blizzard.RealmSlug]miniAuctionList{},
		items:                 map[blizzard.ItemID]item{},
		expansions:            res.config.Expansions,
		professions:           res.config.Professions,
	}
}

type state struct {
	messenger messenger
	resolver  resolver
	listeners listeners
	databases databases

	regions               []region
	statuses              statuses
	auctionIntakeStatuses map[regionName]map[blizzard.RealmSlug]time.Time
	auctions              map[regionName]map[blizzard.RealmSlug]miniAuctionList
	items                 itemsMap
	itemClasses           blizzard.ItemClasses
	expansions            []expansion
	professions           []profession
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

type bootResponse struct {
	Regions     regionList           `json:"regions"`
	ItemClasses blizzard.ItemClasses `json:"item_classes"`
	Expansions  []expansion          `json:"expansions"`
	Professions []profession         `json:"professions"`
}

func (sta state) listenForBoot(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.Boot, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		encodedResponse, err := json.Marshal(bootResponse{
			Regions:     sta.regions,
			ItemClasses: sta.itemClasses,
			Expansions:  sta.expansions,
			Professions: sta.professions,
		})
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedResponse)
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

type auctionsIntakeResult struct {
	itemIds              []blizzard.ItemID
	removedAuctionsCount int
}

func (sta state) auctionsIntake(job getAuctionsJob) (auctionsIntakeResult, error) {
	rea := job.realm
	reg := rea.region

	// setting the realm last-modified
	for i, statusRealm := range sta.statuses[reg.Name].Realms {
		if statusRealm.Slug != rea.Slug {
			continue
		}

		sta.statuses[reg.Name].Realms[i].LastModified = job.lastModified.Unix()

		break
	}

	// gathering item-ids for item fetching
	itemIdsMap := map[blizzard.ItemID]struct{}{}
	for _, auc := range job.auctions.Auctions {
		itemIdsMap[auc.Item] = struct{}{}
	}
	itemIds := make([]blizzard.ItemID, len(itemIdsMap))
	i := 0
	for ID := range itemIdsMap {
		itemIds[i] = ID
		i++
	}

	// returning a list of item ids for syncing
	return auctionsIntakeResult{itemIds: itemIds}, nil
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
