package state

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	nats "github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/metric/kinds"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
)

type PubStateConfig struct {
	GCloudProjectID string

	MessengerHost string
	MessengerPort int

	PricelistHistoriesDatabaseV2Dir string
}

func NewPubStateIO(config PubStateConfig, statuses sotah.Statuses) (IO, error) {
	out := IO{}

	// connecting to the messenger host
	logging.Info("Connecting messenger")
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return IO{}, err
	}
	out.Messenger = mess

	// initializing a reporter
	out.Reporter = metric.NewReporter(mess)

	// establishing a bus
	busClient, err := bus.NewClient(config.GCloudProjectID, "pub")
	out.BusClient = busClient

	// establishing a store
	stor, err := store.NewClient(config.GCloudProjectID)
	if err != nil {
		return IO{}, err
	}
	out.StoreClient = stor

	// loading the pricelist-histories-v2 databases
	phDatabases, err := database.NewPricelistHistoryDatabasesV2(config.PricelistHistoriesDatabaseV2Dir, statuses)
	if err != nil {
		return IO{}, err
	}
	out.Databases.PricelistHistoryDatabasesV2 = phDatabases

	return out, nil
}

func NewPubState(config PubStateConfig) (PubState, error) {
	// establishing an initial state
	pubState := PubState{
		State: NewState(uuid.NewV4(), true),
	}

	// gathering regions
	logging.Info("Gathering regions")
	regions, err := pubState.NewRegions()
	if err != nil {
		return PubState{}, err
	}
	pubState.Regions = regions

	// gathering statuses
	logging.Info("Gathering statuses")
	for _, reg := range pubState.Regions {
		status, err := pubState.NewStatus(reg)
		if err != nil {
			return PubState{}, err
		}

		pubState.Statuses[reg.Name] = status
	}

	// pruning old data
	earliestTime := database.RetentionLimit()
	for regionName, status := range pubState.Statuses {
		regionDatabaseDir := fmt.Sprintf("%s/%s", config.PricelistHistoriesDatabaseV2Dir, regionName)

		for _, rea := range status.Realms {
			realmDatabaseDir := fmt.Sprintf("%s/%s", regionDatabaseDir, rea.Slug)
			dbPaths, err := database.DatabaseV2Paths(realmDatabaseDir)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error": err.Error(),
					"dir":   realmDatabaseDir,
				}).Error("Failed to resolve database paths")

				return PubState{}, err
			}
			for _, dbPathPair := range dbPaths {
				if dbPathPair.TargetTime.After(earliestTime) {
					continue
				}

				logging.WithFields(logrus.Fields{
					"pathname": dbPathPair.FullPath,
				}).Debug("Pruning old pricelist-history database file")

				if err := os.Remove(dbPathPair.FullPath); err != nil {
					logging.WithFields(logrus.Fields{
						"error":    err.Error(),
						"dir":      realmDatabaseDir,
						"pathname": dbPathPair.FullPath,
					}).Error("Failed to remove database file")

					return PubState{}, err
				}
			}
		}
	}

	// connecting IO
	io, err := NewPubStateIO(config, pubState.Statuses)
	if err != nil {
		return PubState{}, err
	}
	pubState.IO = io

	// establishing listeners
	pubState.Listeners = NewListeners(SubjectListeners{
		subjects.PricelistHistoriesIntakeV2: pubState.ListenForPricelistHistoriesIntakeV2,
	})

	return pubState, nil
}

type PubState struct {
	State
}

func newPricelistHistoriesIntakeV2Request(data []byte) (pricelistHistoriesIntakeV2Request, error) {
	pRequest := &pricelistHistoriesIntakeV2Request{}
	err := json.Unmarshal(data, &pRequest)
	if err != nil {
		return pricelistHistoriesIntakeV2Request{}, err
	}

	return *pRequest, nil
}

type pricelistHistoriesIntakeV2Request struct {
	RegionRealmTimestamps sotah.RegionRealmTimestamps `json:"realm_timestamps"`
}

func (pRequest pricelistHistoriesIntakeV2Request) resolve(statuses sotah.Statuses) (RegionRealmTimes, sotah.RegionRealmMap) {
	included := RegionRealmTimes{}
	excluded := sotah.RegionRealmMap{}

	for regionName, status := range statuses {
		excluded[regionName] = sotah.RealmMap{}
		for _, realm := range status.Realms {
			excluded[regionName][realm.Slug] = realm
		}
	}
	for regionName, realmTimestamps := range pRequest.RegionRealmTimestamps {
		included[regionName] = RealmTimes{}
		for realmSlug, timestamp := range realmTimestamps {
			delete(excluded[regionName], realmSlug)

			targetTime := time.Unix(timestamp, 0)
			for _, realm := range statuses[regionName].Realms {
				if realm.Slug != realmSlug {
					continue
				}

				included[regionName][realmSlug] = RealmTimeTuple{
					Realm:      realm,
					TargetTime: targetTime,
				}

				break
			}
		}
	}

	return included, excluded
}

func (pRequest pricelistHistoriesIntakeV2Request) handle(sta PubState) {
	// misc
	startTime := time.Now()

	// resolving included and excluded auctions
	included, excluded := pRequest.resolve(sta.Statuses)

	// counting realms for reporting
	includedRealmCount := func() int {
		out := 0
		for _, realmTimes := range included {
			out += len(realmTimes)
		}

		return out
	}()
	excludedRealmCount := func() int {
		out := 0
		for _, realmsMap := range excluded {
			out += len(realmsMap)
		}

		return out
	}()

	// loading region-realm-timestamps from request into the bus
	sta.IO.BusClient.LoadRegionRealmTimestamps(pRequest.RegionRealmTimestamps)

	duration := time.Now().Sub(startTime)
	durationKind := fmt.Sprintf("%s_duration", kinds.PricelistHistoriesIntakeV2)
	sta.IO.Reporter.Report(metric.Metrics{
		durationKind:      int(duration) / 1000 / 1000 / 1000,
		"included_realms": includedRealmCount,
		"excluded_realms": excludedRealmCount,
		"total_realms":    includedRealmCount + excludedRealmCount,
	})

	return
}

func (pubState PubState) ListenForPricelistHistoriesIntakeV2(stop ListenStopChan) error {
	in := make(chan pricelistHistoriesIntakeV2Request, 30)

	err := pubState.IO.Messenger.Subscribe(string(subjects.PricelistHistoriesIntakeV2), stop, func(natsMsg nats.Msg) {
		// resolving the request
		pRequest, err := newPricelistHistoriesIntakeV2Request(natsMsg.Data)
		if err != nil {
			logging.WithField("error", err.Error()).Error("Failed to parse pricelist-histories-intake-request")

			return
		}

		pubState.IO.Reporter.ReportWithPrefix(metric.Metrics{
			"buffer_size": len(pRequest.RegionRealmTimestamps),
		}, kinds.PricelistHistoriesIntakeV2)
		logging.WithField("capacity", len(in)).Info("Received pricelist-histories-intake-v2-request, pushing onto handle channel")

		in <- pRequest
	})
	if err != nil {
		return err
	}

	// starting up a worker to handle pricelist-histories-intake-v2 requests
	go func() {
		for pRequest := range in {
			pRequest.handle(pubState)
		}
	}()

	return nil
}
