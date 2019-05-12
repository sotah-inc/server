package state

import (
	"encoding/json"
	"time"

	nats "github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/metric/kinds"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func newPricelistHistoriesIntakeRequest(data []byte) (pricelistHistoriesIntakeRequest, error) {
	pRequest := &pricelistHistoriesIntakeRequest{}
	err := json.Unmarshal(data, &pRequest)
	if err != nil {
		return pricelistHistoriesIntakeRequest{}, err
	}

	return *pRequest, nil
}

type pricelistHistoriesIntakeRequest struct {
	RegionRealmTimestamps sotah.RegionRealmTimestampMaps `json:"realm_timestamps"`
}

func (pRequest pricelistHistoriesIntakeRequest) resolve(statuses sotah.Statuses) (RegionRealmTimes, sotah.RegionRealmMap) {
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

func (pRequest pricelistHistoriesIntakeRequest) handle(sta PricelistHistoriesState) {
	// misc
	startTime := time.Now()

	// declaring a load-in channel for the pricelist-histories db and starting it up
	loadInJobs := make(chan database.LoadInJob)
	loadOutJobs := sta.IO.Databases.PricelistHistoryDatabases.Load(loadInJobs)

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

	// spinning up a goroutine for gathering auctions
	go func() {
		for getAuctionsFromTimesJob := range sta.GetAuctionsFromTimes(included) {
			if getAuctionsFromTimesJob.Err != nil {
				logrus.WithFields(getAuctionsFromTimesJob.ToLogrusFields()).Error("Failed to fetch auctions")

				continue
			}

			loadInJobs <- database.LoadInJob{
				Realm:      getAuctionsFromTimesJob.Realm,
				TargetTime: getAuctionsFromTimesJob.TargetTime,
				Auctions:   getAuctionsFromTimesJob.Auctions,
			}
		}

		// closing the load-in channel
		close(loadInJobs)
	}()

	// gathering load-out-jobs as they drain
	for loadOutJob := range loadOutJobs {
		if loadOutJob.Err != nil {
			logrus.WithFields(loadOutJob.ToLogrusFields()).Error("Failed to load auctions")

			continue
		}
	}

	duration := time.Now().Sub(startTime)
	sta.IO.Reporter.Report(metric.Metrics{
		"pricelisthistories_intake_duration": int(duration) / 1000 / 1000 / 1000,
		"included_realms":                    includedRealmCount,
		"excluded_realms":                    excludedRealmCount,
		"total_realms":                       includedRealmCount + excludedRealmCount,
	})
}

func (sta PricelistHistoriesState) ListenForPricelistHistoriesIntake(stop ListenStopChan) error {
	in := make(chan pricelistHistoriesIntakeRequest, 30)

	// starting up a listener for pricelist-histories-intake
	err := sta.IO.Messenger.Subscribe(string(subjects.PricelistHistoriesIntake), stop, func(natsMsg nats.Msg) {
		// resolving the request
		pRequest, err := newPricelistHistoriesIntakeRequest(natsMsg.Data)
		if err != nil {
			logging.WithField("error", err.Error()).Error("Failed to parse pricelist-histories-intake-request")

			return
		}

		sta.IO.Reporter.ReportWithPrefix(metric.Metrics{
			"buffer_size": len(pRequest.RegionRealmTimestamps),
		}, kinds.PricelistHistoriesIntake)
		logging.WithField("capacity", len(in)).Info("Received pricelist-histories-intake-request, pushing onto handle channel")

		in <- pRequest
	})
	if err != nil {
		return err
	}

	// starting up a worker to handle pricelist-histories-intake requests
	go func() {
		for pRequest := range in {
			pRequest.handle(sta)
		}
	}()

	return nil
}
