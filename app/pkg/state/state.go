package state

import (
	"github.com/sotah-inc/server/app/pkg/diskstore"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/store"
	"google.golang.org/grpc/resolver"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/twinj/uuid"
)

type requestError struct {
	code    codes.Code
	message string
}

type ItemBlacklistMap map[blizzard.ItemID]struct{}

// derived command-specific state
type APIState struct {
	State

	SessionSecret uuid.UUID

	Regions       []sotah.Region
	Statuses      sotah.Statuses
	ItemClasses   blizzard.ItemClasses
	expansions    []sotah.Expansion
	professions   []sotah.Profession
	ItemBlacklist ItemBlacklistMap
}

// databases
type Databases struct {
	PricelistHistoryDatabases database.PricelistHistoryDatabases
	LiveAuctionsDatabases     database.LiveAuctionsDatabases
	ItemsDatabase             database.ItemsDatabase
}

// io bundle
type IO struct {
	resolver  resolver.Resolver
	databases Databases
	messenger messenger.Messenger
	store     store.Store
	diskStore diskstore.DiskStore
}

// listener functionality
type listener struct {
	call     listenFunc
	stopChan ListenStopChan
}

type ListenStopChan chan interface{}

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
	logging.WithField("Listeners", len(ls)).Info("Starting Listeners")

	for _, l := range ls {
		if err := l.call(l.stopChan); err != nil {
			return err
		}
	}

	return nil
}

func (ls Listeners) Stop() {
	logging.Info("Stopping Listeners")

	for _, l := range ls {
		l.stopChan <- struct{}{}
	}
}

// state
func NewState(runId uuid.UUID, ls Listeners) State {
	return State{RunID: runId, Listeners: ls}
}

type State struct {
	RunID     uuid.UUID
	Listeners Listeners
	IO        IO
}
