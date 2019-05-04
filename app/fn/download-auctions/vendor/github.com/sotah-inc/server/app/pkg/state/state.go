package state

import (
	"encoding/json"
	"time"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	dCodes "github.com/sotah-inc/server/app/pkg/database/codes"
	"github.com/sotah-inc/server/app/pkg/diskstore"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/resolver"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
)

type requestError struct {
	code    mCodes.Code
	message string
}

// databases
type Databases struct {
	PricelistHistoryDatabases database.PricelistHistoryDatabases
	LiveAuctionsDatabases     database.LiveAuctionsDatabases
	ItemsDatabase             database.ItemsDatabase
	MetaDatabase              database.MetaDatabase
}

// io bundle
type IO struct {
	Resolver    resolver.Resolver
	Databases   Databases
	Messenger   messenger.Messenger
	StoreClient store.Client
	DiskStore   diskstore.DiskStore
	Reporter    metric.Reporter
	BusClient   bus.Client
}

// bus-listener functionality
type busListenFunc func(onReady chan interface{}, stop chan interface{}, onStopped chan interface{})

type busListener struct {
	call      busListenFunc
	onReady   chan interface{}
	stop      chan interface{}
	onStopped chan interface{}
}

type SubjectBusListeners map[subjects.Subject]busListenFunc

func NewBusListeners(sListeners SubjectBusListeners) BusListeners {
	out := BusListeners{}
	for subj, l := range sListeners {
		out[subj] = busListener{
			call:      l,
			onStopped: make(chan interface{}),
			onReady:   make(chan interface{}),
			stop:      make(chan interface{}),
		}
	}

	return out
}

type BusListeners map[subjects.Subject]busListener

func (ls BusListeners) Listen() {
	logging.WithField("count", len(ls)).Info("Starting bus-listeners")

	for _, l := range ls {
		l.call(l.onReady, l.stop, l.onStopped)
		<-l.onReady
	}
}

func (ls BusListeners) Stop() {
	logging.WithField("count", len(ls)).Info("Stopping bus-listeners")

	for _, l := range ls {
		l.stop <- struct{}{}
		<-l.onStopped
	}
}

// listener functionality
type ListenStopChan chan interface{}

type listenFunc func(stop ListenStopChan) error

type listener struct {
	call     listenFunc
	stopChan ListenStopChan
}

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
	RunID        uuid.UUID
	Listeners    Listeners
	BusListeners BusListeners
	UseGCloud    bool

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

func DatabaseCodeToMessengerCode(dCode dCodes.Code) mCodes.Code {
	switch dCode {
	case dCodes.Ok:
		return mCodes.Ok
	case dCodes.Blank:
		return mCodes.Blank
	case dCodes.GenericError:
		return mCodes.GenericError
	case dCodes.MsgJSONParseError:
		return mCodes.MsgJSONParseError
	case dCodes.NotFound:
		return mCodes.NotFound
	case dCodes.UserError:
		return mCodes.UserError
	}

	return mCodes.Blank
}

func NewRealmModificationDatesRequest(data []byte) (RealmModificationDatesRequest, error) {
	var r RealmModificationDatesRequest
	if err := json.Unmarshal(data, &r); err != nil {
		return RealmModificationDatesRequest{}, err
	}

	return r, nil
}

type RealmModificationDatesRequest struct {
	RegionName string `json:"region_name"`
	RealmSlug  string `json:"realm_slug"`
}

type RealmModificationDatesResponse struct {
	sotah.RealmModificationDates
}

func (r RealmModificationDatesResponse) EncodeForDelivery() ([]byte, error) {
	return json.Marshal(r)
}
