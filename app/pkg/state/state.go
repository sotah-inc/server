package state

import (
	"time"

	"github.com/sotah-inc/server/app/pkg/database"

	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/twinj/uuid"
)

type requestError struct {
	code    codes.Code
	message string
}

func newState(mess messenger.Messenger, res internal.Resolver) State {
	return State{
		Messenger:             mess,
		resolver:              res,
		Regions:               res.Config.FilterInRegions(res.Config.Regions),
		Statuses:              internal.Statuses{},
		auctionIntakeStatuses: map[internal.RegionName]map[blizzard.RealmSlug]time.Time{},
		expansions:            res.Config.Expansions,
		professions:           res.Config.Professions,
		itemBlacklist:         newItemBlacklistMap(res.Config.ItemBlacklist),
	}
}

type State struct {
	Messenger                 messenger.Messenger
	resolver                  internal.Resolver
	listeners                 listeners
	pricelistHistoryDatabases database.PricelistHistoryDatabases
	liveAuctionsDatabases     database.LiveAuctionsDatabases
	itemsDatabase             database.ItemsDatabase
	sessionSecret             uuid.UUID
	runID                     uuid.UUID

	Regions               []internal.Region
	Statuses              internal.Statuses
	auctionIntakeStatuses map[internal.RegionName]map[blizzard.RealmSlug]time.Time
	itemClasses           blizzard.ItemClasses
	expansions            []internal.Expansion
	professions           []internal.Profession
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

type ListenStopChan chan interface{}

type listenFunc func(stop ListenStopChan) error

type subjectListeners map[subjects.Subject]listenFunc

func newListeners(sListeners subjectListeners) listeners {
	ls := listeners{}
	for subj, l := range sListeners {
		ls[subj] = listener{l, make(ListenStopChan)}
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
	stopChan ListenStopChan
}

type WorkerStopChan chan interface{}
