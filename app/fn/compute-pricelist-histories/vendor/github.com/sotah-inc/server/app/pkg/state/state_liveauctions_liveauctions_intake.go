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

func newLiveAuctionsIntakeRequest(data []byte) (liveAuctionsIntakeRequest, error) {
	iRequest := &liveAuctionsIntakeRequest{}
	err := json.Unmarshal(data, &iRequest)
	if err != nil {
		return liveAuctionsIntakeRequest{}, err
	}

	return *iRequest, nil
}

type liveAuctionsIntakeRequest struct {
	RegionRealmTimestamps sotah.RegionRealmTimestamps `json:"realm_timestamps"`
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

func (iRequest liveAuctionsIntakeRequest) handle(laState LiveAuctionsState) {
	// misc
	startTime := time.Now()

	// declaring a load-in channel for the live-auctions db and starting it up
	loadInJobs := make(chan database.LoadInJob)
	loadOutJobs := laState.IO.Databases.LiveAuctionsDatabases.Load(loadInJobs)

	// resolving included and excluded auctions
	included, excluded := iRequest.resolve(laState.Statuses)

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

	// gathering stats for further data gathering
	totalPreviousAuctions := 0
	totalAuctions := 0
	totalOwners := 0
	itemIdsMap := sotah.ItemIdsMap{}
	for _, realmsMap := range excluded {
		for getStatsJob := range laState.IO.Databases.LiveAuctionsDatabases.GetStats(realmsMap.ToRealms()) {
			if getStatsJob.Err != nil {
				logrus.WithFields(getStatsJob.ToLogrusFields()).Error("Failed to get live-auction stats")

				continue
			}

			totalPreviousAuctions += getStatsJob.Stats.TotalAuctions
			totalAuctions += getStatsJob.Stats.TotalAuctions
			totalOwners += len(getStatsJob.Stats.OwnerNames)
			for _, itemId := range getStatsJob.Stats.ItemIds {
				itemIdsMap[itemId] = struct{}{}
			}
		}
	}

	// spinning up a goroutine for gathering auctions
	go func() {
		for getAuctionsFromTimesJob := range laState.GetAuctionsFromTimes(included) {
			if getAuctionsFromTimesJob.Err != nil {
				logrus.WithFields(getAuctionsFromTimesJob.ToLogrusFields()).Error("Failed to fetch auctions")

				continue
			}

			totalAuctions += len(getAuctionsFromTimesJob.Auctions.Auctions)
			totalOwners += len(getAuctionsFromTimesJob.Auctions.OwnerNames())
			for _, auc := range getAuctionsFromTimesJob.Auctions.Auctions {
				itemIdsMap[auc.Item] = struct{}{}
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
	totalNewAuctions := 0
	totalRemovedAuctions := 0
	for loadOutJob := range loadOutJobs {
		if loadOutJob.Err != nil {
			logrus.WithFields(loadOutJob.ToLogrusFields()).Error("Failed to load auctions")

			continue
		}

		totalNewAuctions += loadOutJob.TotalNewAuctions
		totalRemovedAuctions += loadOutJob.TotalRemovedAuctions
	}

	// publishing for pricelist-histories-intake
	err := func() error {
		if laState.UseGCloud {
			phiRequest := pricelistHistoriesIntakeV2Request{RegionRealmTimestamps: iRequest.RegionRealmTimestamps}
			return func() error {
				encodedRequest, err := json.Marshal(phiRequest)
				if err != nil {
					return err
				}

				return laState.IO.Messenger.Publish(string(subjects.PricelistHistoriesIntakeV2), encodedRequest)
			}()
		}

		phiRequest := pricelistHistoriesIntakeRequest{RegionRealmTimestamps: iRequest.RegionRealmTimestamps}
		return func() error {
			encodedRequest, err := json.Marshal(phiRequest)
			if err != nil {
				return err
			}

			return laState.IO.Messenger.Publish(string(subjects.PricelistHistoriesIntake), encodedRequest)
		}()
	}()
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to publish pricelist-histories-intake request")

		return
	}

	duration := time.Now().Sub(startTime)
	laState.IO.Reporter.Report(metric.Metrics{
		"liveauctions_intake_duration": int(duration) / 1000 / 1000 / 1000,
		"included_realms":              includedRealmCount,
		"excluded_realms":              excludedRealmCount,
		"total_realms":                 includedRealmCount + excludedRealmCount,
		"total_auctions":               totalAuctions,
		"total_previous_auctions":      totalPreviousAuctions,
		"total_owners":                 totalOwners,
		"total_items":                  len(itemIdsMap),
		"total_new_auctions":           totalNewAuctions,
		"total_removed_auctions":       totalRemovedAuctions,
	})

	return
}

func (laState LiveAuctionsState) ListenForLiveAuctionsIntake(stop ListenStopChan) error {
	in := make(chan liveAuctionsIntakeRequest, 30)

	// starting up a listener for live-auctions-intake
	err := laState.IO.Messenger.Subscribe(string(subjects.LiveAuctionsIntake), stop, func(natsMsg nats.Msg) {
		// resolving the request
		iRequest, err := newLiveAuctionsIntakeRequest(natsMsg.Data)
		if err != nil {
			logging.WithField("error", err.Error()).Error("Failed to parse live-auctions-intake-request")

			return
		}

		laState.IO.Reporter.ReportWithPrefix(metric.Metrics{
			"buffer_size": len(iRequest.RegionRealmTimestamps),
		}, kinds.LiveAuctionsIntake)
		logging.WithField("capacity", len(in)).Info("Received live-auctions-intake-request, pushing onto handle channel")

		in <- iRequest
	})
	if err != nil {
		return err
	}

	// starting up a worker to handle live-auctions-intake requests
	go func() {
		for iRequest := range in {
			iRequest.handle(laState)
		}
	}()

	return nil
}
