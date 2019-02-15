package state

import (
	"time"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/diskstore"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/resolver"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
)

type requestError struct {
	code    codes.Code
	message string
}

// databases
type Databases struct {
	PricelistHistoryDatabases database.PricelistHistoryDatabases
	LiveAuctionsDatabases     database.LiveAuctionsDatabases
	ItemsDatabase             database.ItemsDatabase
}

// io bundle
type IO struct {
	Resolver  resolver.Resolver
	Databases Databases
	Messenger messenger.Messenger
	Store     store.Store
	DiskStore diskstore.DiskStore
	Reporter  metric.Reporter
	BusClient bus.Client
}

// listener functionality
type ListenStopChan chan interface{}

type listener struct {
	call     listenFunc
	stopChan ListenStopChan
}

type listenFunc func(stop ListenStopChan) error

type SubjectListeners map[subjects.Subject]listenFunc

func NewListeners(sListeners SubjectListeners) Listeners {
	ls := Listeners{}
	for subj, l := range sListeners {
		ls[subj] = listener{l, make(ListenStopChan)}
	}

	return ls
}

type Listeners map[subjects.Subject]listener

func (ls Listeners) Listen() error {
	logging.WithField("listeners", len(ls)).Info("Starting listeners")

	for _, l := range ls {
		if err := l.call(l.stopChan); err != nil {
			return err
		}
	}

	return nil
}

func (ls Listeners) Stop() {
	logging.Info("Stopping listeners")

	for _, l := range ls {
		l.stopChan <- struct{}{}
	}
}

// state
func NewState(runId uuid.UUID, useGCloud bool) State {
	return State{RunID: runId, UseGCloud: useGCloud, Statuses: sotah.Statuses{}}
}

type State struct {
	RunID     uuid.UUID
	Listeners Listeners
	UseGCloud bool

	IO IO

	Regions  sotah.RegionList
	Statuses sotah.Statuses
}

type RealmTimeTuple struct {
	Realm      sotah.Realm
	TargetTime time.Time
}

type RealmTimes map[blizzard.RealmSlug]RealmTimeTuple

type RegionRealmTimes map[blizzard.RegionName]RealmTimes

type RealmTimestamps map[blizzard.RealmSlug]int64

type RegionRealmTimestamps map[blizzard.RegionName]RealmTimestamps
