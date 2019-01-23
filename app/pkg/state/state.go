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

func NewState(mess messenger.Messenger, res internal.Resolver) State {
	return State{
		Messenger:             mess,
		Resolver:              res,
		Regions:               res.Config.FilterInRegions(res.Config.Regions),
		Statuses:              internal.Statuses{},
		auctionIntakeStatuses: map[internal.RegionName]map[blizzard.RealmSlug]time.Time{},
		expansions:            res.Config.Expansions,
		professions:           res.Config.Professions,
		ItemBlacklist:         newItemBlacklistMap(res.Config.ItemBlacklist),
	}
}

type State struct {
	Messenger                 messenger.Messenger
	Resolver                  internal.Resolver
	Listeners                 listeners
	PricelistHistoryDatabases database.PricelistHistoryDatabases
	LiveAuctionsDatabases     database.LiveAuctionsDatabases
	ItemsDatabase             database.ItemsDatabase
	SessionSecret             uuid.UUID
	RunID                     uuid.UUID

	Regions               []internal.Region
	Statuses              internal.Statuses
	auctionIntakeStatuses map[internal.RegionName]map[blizzard.RealmSlug]time.Time
	ItemClasses           blizzard.ItemClasses
	expansions            []internal.Expansion
	professions           []internal.Profession
	ItemBlacklist         ItemBlacklistMap
}

func newItemBlacklistMap(IDs []blizzard.ItemID) ItemBlacklistMap {
	out := ItemBlacklistMap{}

	if len(IDs) == 0 {
		return out
	}

	for _, ID := range IDs {
		out[ID] = struct{}{}
	}

	return out
}

type ItemBlacklistMap map[blizzard.ItemID]struct{}

type ListenStopChan chan interface{}

type listenFunc func(stop ListenStopChan) error

type SubjectListeners map[subjects.Subject]listenFunc

func NewListeners(sListeners SubjectListeners) listeners {
	ls := listeners{}
	for subj, l := range sListeners {
		ls[subj] = listener{l, make(ListenStopChan)}
	}

	return ls
}

type listeners map[subjects.Subject]listener

func (ls listeners) Listen() error {
	logging.WithField("Listeners", len(ls)).Info("Starting Listeners")

	for _, l := range ls {
		if err := l.call(l.stopChan); err != nil {
			return err
		}
	}

	return nil
}

func (ls listeners) Stop() {
	logging.Info("Stopping Listeners")

	for _, l := range ls {
		l.stopChan <- struct{}{}
	}
}

type listener struct {
	call     listenFunc
	stopChan ListenStopChan
}

type WorkerStopChan chan interface{}
