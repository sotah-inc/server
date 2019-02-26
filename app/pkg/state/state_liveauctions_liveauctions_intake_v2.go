package state

import (
	"encoding/json"
	"time"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/metric/kinds"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func newLiveAuctionsIntakeV2Request(data []byte) (liveAuctionsIntakeV2Request, error) {
	iRequest := &liveAuctionsIntakeV2Request{}
	err := json.Unmarshal(data, &iRequest)
	if err != nil {
		return liveAuctionsIntakeV2Request{}, err
	}

	return *iRequest, nil
}

type liveAuctionsIntakeV2Request struct {
	RegionRealmTimestamps sotah.RegionRealmTimestamps `json:"realm_timestamps"`
}

func (iRequest liveAuctionsIntakeV2Request) resolve(statuses sotah.Statuses) (RegionRealmTimes, sotah.RegionRealmMap) {
	included := RegionRealmTimes{}
	excluded := sotah.RegionRealmMap{}

	for regionName, status := range statuses {
		excluded[regionName] = sotah.RealmMap{}
		for _, realm := range status.Realms {
			excluded[regionName][realm.Slug] = realm
		}
	}
	for regionName, realmTimestamps := range iRequest.RegionRealmTimestamps {
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

func (iRequest liveAuctionsIntakeV2Request) handle(sta LiveAuctionsState) {
	// misc
	startTime := time.Now()

	// resolving included and excluded auctions
	included, excluded := iRequest.resolve(sta.Statuses)

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
	sta.IO.BusClient.LoadRegionRealmTimestamps(iRequest.RegionRealmTimestamps, subjects.LiveAuctionsCompute)

	duration := time.Now().Sub(startTime)
	sta.IO.Reporter.Report(metric.Metrics{
		"liveauctions_intake_v2_duration": int(duration) / 1000 / 1000 / 1000,
		"included_realms":                 includedRealmCount,
		"excluded_realms":                 excludedRealmCount,
		"total_realms":                    includedRealmCount + excludedRealmCount,
	})

	return
}

func (sta LiveAuctionsState) ListenForLiveAuctionsIntakeV2(stop ListenStopChan) error {
	in := make(chan liveAuctionsIntakeV2Request, 30)

	// starting up a listener for live-auctions-intake
	err := sta.IO.Messenger.Subscribe(string(subjects.LiveAuctionsIntakeV2), stop, func(natsMsg nats.Msg) {
		// resolving the request
		iRequest, err := newLiveAuctionsIntakeV2Request(natsMsg.Data)
		if err != nil {
			logging.WithField("error", err.Error()).Error("Failed to parse live-auctions-intake-v2-request")

			return
		}

		sta.IO.Reporter.ReportWithPrefix(metric.Metrics{
			"buffer_size": len(iRequest.RegionRealmTimestamps),
		}, kinds.LiveAuctionsIntakeV2)
		logging.WithField("capacity", len(in)).Info("Received live-auctions-intake-v2-request, pushing onto handle channel")

		in <- iRequest
	})
	if err != nil {
		return err
	}

	// starting up a worker to handle live-auctions-intake requests
	go func() {
		for iRequest := range in {
			iRequest.handle(sta)
		}
	}()

	return nil
}
