package state

import (
	"encoding/json"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/metric/kinds"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func newPricelistHistoriesComputeIntakeRequest(data []byte) (PricelistHistoriesComputeIntakeRequest, error) {
	pRequest := &PricelistHistoriesComputeIntakeRequest{}
	err := json.Unmarshal(data, &pRequest)
	if err != nil {
		return PricelistHistoriesComputeIntakeRequest{}, err
	}

	return *pRequest, nil
}

type PricelistHistoriesComputeIntakeRequest struct {
	RegionName                string `json:"region_name"`
	RealmSlug                 string `json:"realm_slug"`
	NormalizedTargetTimestamp int    `json:"normalized_target_timestamp"`
}

func (pRequest PricelistHistoriesComputeIntakeRequest) handle(pubState PubState, loadInJobs chan database.PricelistHistoryDatabaseV2LoadInJob) {
	logging.WithFields(logrus.Fields{
		"region_name":                 pRequest.RegionName,
		"realm_slug":                  pRequest.RealmSlug,
		"normalized_target_timestamp": pRequest.NormalizedTargetTimestamp,
	}).Info("Handling request")

	loadInJobs <- database.PricelistHistoryDatabaseV2LoadInJob{
		RegionName:                blizzard.RegionName(pRequest.RegionName),
		RealmSlug:                 blizzard.RealmSlug(pRequest.RealmSlug),
		NormalizedTargetTimestamp: sotah.UnixTimestamp(pRequest.NormalizedTargetTimestamp),
	}
}

func (pubState PubState) ListenForPricelistHistoriesComputeIntake(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// declaring a load-in channel for the pricelist-histories-v2 db and starting it up
	loadInJobs := make(chan database.PricelistHistoryDatabaseV2LoadInJob)
	loadOutJobs := pubState.IO.Databases.PricelistHistoryDatabasesV2.Load(loadInJobs)
	go func() {
		for job := range loadOutJobs {
			if job.Err != nil {
				logging.WithFields(job.ToLogrusFields()).Error("Failed to load job")

				continue
			}
		}
	}()

	// starting up a worker to handle pricelist-histories-compute-intake requests
	total := func() int {
		out := 0
		for _, status := range pubState.Statuses {
			out += len(status.Realms)
		}

		return out
	}()
	in := make(chan PricelistHistoriesComputeIntakeRequest, total)
	go func() {
		for pRequest := range in {
			pRequest.handle(pubState, loadInJobs)
		}
	}()

	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			// resolving the request
			pRequest, err := newPricelistHistoriesComputeIntakeRequest([]byte(busMsg.Data))
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to parse pricelist-histories-compute-intake-request")

				return
			}

			pubState.IO.Reporter.ReportWithPrefix(metric.Metrics{
				"buffer_size": len(in),
			}, kinds.PricelistHistoriesComputeIntake)
			logging.WithField("capacity", len(in)).Info("Received pricelist-histories-compute-intake-request, pushing onto handle channel")

			in <- pRequest
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := pubState.IO.BusClient.SubscribeToTopic(string(subjects.PricelistHistoriesComputeIntake), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
