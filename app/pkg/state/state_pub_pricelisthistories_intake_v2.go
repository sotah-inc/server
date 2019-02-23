package state

import (
	"encoding/json"
	"fmt"
	"time"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/metric/kinds"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

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
			"buffer_size": len(in),
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
