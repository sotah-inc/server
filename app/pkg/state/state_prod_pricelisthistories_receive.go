package state

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
)

func HandleComputedPricelistHistories(
	phState ProdPricelistHistoriesState,
	requests []database.PricelistHistoriesComputeIntakeRequest,
) {
	// declaring a get-in channel for gathering pricelist-histories
	getInJobs := make(chan store.GetAllPricelistHistoriesInJob)
	getOutJobs := phState.PricelistHistoriesBase.GetAll(getInJobs, phState.PricelistHistoriesBucket)

	// declaring a load-in channel for the pricelist-histories db
	loadInJobs := make(chan database.PricelistHistoryDatabaseEncodedLoadInJob)
	loadOutJobs := phState.IO.Databases.PricelistHistoryDatabases.LoadEncoded(loadInJobs)

	// spinning up a worker for translating get-out-jobs to load-in-jobs
	go func() {
		for outJob := range getOutJobs {
			if outJob.Err != nil {
				logging.WithFields(outJob.ToLogrusFields()).Error("Failed to get pricelist-histories")

				continue
			}

			loadInJobs <- database.PricelistHistoryDatabaseEncodedLoadInJob{
				RegionName:                outJob.RegionName,
				RealmSlug:                 outJob.RealmSlug,
				NormalizedTargetTimestamp: outJob.TargetTimestamp,
				Data:                      outJob.Data,
				VersionId:                 outJob.VersionId,
			}
		}

		close(loadInJobs)
	}()

	// queueing it all up
	go func() {
		for _, request := range requests {
			logging.WithFields(logrus.Fields{
				"region":                      request.RegionName,
				"realm":                       request.RealmSlug,
				"normalized-target-timestamp": request.NormalizedTargetTimestamp,
			}).Info("Loading request")

			getInJobs <- store.GetAllPricelistHistoriesInJob{
				RegionName:      blizzard.RegionName(request.RegionName),
				RealmSlug:       blizzard.RealmSlug(request.RealmSlug),
				TargetTimestamp: sotah.UnixTimestamp(request.NormalizedTargetTimestamp),
			}
		}

		close(getInJobs)
	}()

	// waiting for the results to drain out
	for job := range loadOutJobs {
		if job.Err != nil {
			logging.WithFields(job.ToLogrusFields()).Error("Failed to load job")

			continue
		}

		logging.WithFields(logrus.Fields{
			"region": job.RegionName,
			"realm":  job.RealmSlug,
		}).Info("Loaded job")

		err := phState.IO.Databases.MetaDatabase.SetPricelistHistoriesVersion(
			job.RegionName,
			job.RealmSlug,
			job.NormalizedTargetTimestamp,
			job.VersionId,
		)
		if err != nil {
			logging.WithFields(job.ToLogrusFields()).Error("Failed to persist pricelist-histories version")

			continue
		}
	}
}

func (phState ProdPricelistHistoriesState) ListenForComputedPricelistHistories(
	onReady chan interface{},
	stop chan interface{},
	onStopped chan interface{},
) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			requests, err := database.NewPricelistHistoriesComputeIntakeRequests(busMsg.Data)
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to decode compute-intake requests")

				return
			}

			// handling requests
			logging.WithField("requests", len(requests)).Info("Received requests")
			startTime := time.Now()
			HandleComputedPricelistHistories(phState, requests)
			logging.WithField("requests", len(requests)).Info("Done handling requests")

			// reporting metrics
			m := metric.Metrics{"receive_all_pricelist_histories_duration": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000)}
			if err := phState.IO.BusClient.PublishMetrics(m); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to publish metric")

				return
			}

			return
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := phState.IO.BusClient.SubscribeToTopic(string(subjects.ReceiveComputedPricelistHistories), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
