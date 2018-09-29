package main

import (
	"time"

	"github.com/twinj/uuid"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/subjects"
)

type requestError struct {
	code    codes.Code
	message string
}

func newState(mess messenger, res resolver) state {
	return state{
		messenger:             mess,
		resolver:              res,
		regions:               res.config.filterInRegions(res.config.Regions),
		statuses:              statuses{},
		auctionIntakeStatuses: map[regionName]map[blizzard.RealmSlug]time.Time{},
		auctions:              map[regionName]map[blizzard.RealmSlug]miniAuctionList{},
		items:                 map[blizzard.ItemID]item{},
		expansions:            res.config.Expansions,
		professions:           res.config.Professions,
		itemBlacklist:         newItemBlacklistMap(res.config.ItemBlacklist),
	}
}

type state struct {
	messenger                 messenger
	resolver                  resolver
	listeners                 listeners
	pricelistHistoryDatabases pricelistHistoryDatabases
	liveAuctionsDatabases     liveAuctionsDatabases
	sessionSecret             uuid.Uuid

	regions               []region
	statuses              statuses
	auctionIntakeStatuses map[regionName]map[blizzard.RealmSlug]time.Time
	auctions              map[regionName]map[blizzard.RealmSlug]miniAuctionList
	items                 itemsMap
	itemClasses           blizzard.ItemClasses
	expansions            []expansion
	professions           []profession
	itemBlacklist         itemBlacklistMap
}

func newItemBlacklistMap(IDs []blizzard.ItemID) itemBlacklistMap {
	out := itemBlacklistMap{}

	if len(IDs) == 0 {
		return out
	}

	for _, ID := range IDs {
		out[ID] = struct{}{}
	}

	return out
}

type itemBlacklistMap map[blizzard.ItemID]struct{}

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
	logging.WithField("listeners", len(ls)).Info("Starting listeners")

	for _, l := range ls {
		if err := l.call(l.stopChan); err != nil {
			return err
		}
	}

	return nil
}

func (ls listeners) stop() {
	logging.Info("Stopping listeners")

	for _, l := range ls {
		l.stopChan <- struct{}{}
	}
}

type listener struct {
	call     listenFunc
	stopChan listenStopChan
}

type workerStopChan chan interface{}
