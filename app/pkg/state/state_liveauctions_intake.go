package state

import (
	"encoding/json"
	"time"

	nats "github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func newLiveAuctionsIntakeRequest(data []byte) (liveAuctionsIntakeRequest, error) {
	iRequest := &liveAuctionsIntakeRequest{}
	err := json.Unmarshal(data, &iRequest)
	if err != nil {
		return liveAuctionsIntakeRequest{}, err
	}

	return *iRequest, nil
}

type liveAuctionsIntakeRequest struct {
	RegionRealmTimestamps RegionRealmTimestamps `json:"realm_timestamps"`
}

func (iRequest liveAuctionsIntakeRequest) resolve(statuses sotah.Statuses) (RegionRealmTimes, sotah.RegionRealmMap) {
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

func (iRequest liveAuctionsIntakeRequest) handle(sta State) {
	// misc
	startTime := time.Now()

	// declaring a load-in channel for the live-auctions db and starting it up
	loadInJobs := make(chan database.LoadInJob)
	loadOutJobs := sta.IO.databases.LiveAuctionsDatabases.Load(loadInJobs)

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

	// gathering auctions
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

	// gathering load-out-jobs as they drain
	for loadOutJob := range loadOutJobs {
		if loadOutJob.Err != nil {
			logrus.WithFields(loadOutJob.ToLogrusFields()).Error("Failed to load auctions")

			continue
		}
	}

	metric.ReportDuration(metric.AuctionsIntakeDuration, metric.DurationMetrics{
		Duration:       time.Now().Sub(startTime),
		IncludedRealms: includedRealmCount,
		ExcludedRealms: excludedRealmCount,
		TotalRealms:    includedRealmCount + excludedRealmCount,
	}, logrus.Fields{})

	return
}

func (sta State) ListenForLiveAuctionsIntake(stop messenger.ListenStopChan) error {
	in := make(chan liveAuctionsIntakeRequest, 30)

	// starting up a listener for live-auctions-intake
	err := sta.IO.messenger.Subscribe(subjects.LiveAuctionsIntake, stop, func(natsMsg nats.Msg) {
		// resolving the request
		iRequest, err := newLiveAuctionsIntakeRequest(natsMsg.Data)
		if err != nil {
			logging.WithField("error", err.Error()).Error("Failed to parse live-auctions-intake-request")

			return
		}

		metric.ReportIntakeBufferSize(metric.LiveAuctionsIntake, len(iRequest.RegionRealmTimestamps))
		logging.WithField("capacity", len(in)).Info("Received auctions-intake-request, pushing onto handle channel")

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
