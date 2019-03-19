package state

import (
	"encoding/base64"
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
)

func HandleComputedPricelistHistories(
	phState ProdPricelistHistoriesState,
	requests []database.PricelistHistoriesComputeIntakeRequest,
) {
	// declaring a load-in channel for the live-auctions db and starting it up
	loadInJobs := make(chan database.PricelistHistoryDatabaseEncodedLoadInJob)
	loadOutJobs := phState.IO.Databases.PricelistHistoryDatabases.LoadEncoded(loadInJobs)

	// starting workers for handling tuples
	in := make(chan database.PricelistHistoriesComputeIntakeRequest)
	worker := func() {
		for request := range in {
			// resolving the realm from the request
			realm, err := func() (sotah.Realm, error) {
				for regionName, status := range phState.Statuses {
					if regionName != blizzard.RegionName(request.RegionName) {
						continue
					}

					for _, realm := range status.Realms {
						if realm.Slug != blizzard.RealmSlug(request.RealmSlug) {
							continue
						}

						return realm, nil
					}
				}

				return sotah.Realm{}, errors.New("realm not found")
			}()
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to resolve realm from tuple")

				continue
			}

			// resolving the data
			data, err := func() (map[blizzard.ItemID][]byte, error) {
				obj, err := phState.PricelistHistoriesBase.GetFirmObject(
					time.Unix(int64(request.NormalizedTargetTimestamp), 0),
					realm,
					phState.PricelistHistoriesBucket,
				)
				if err != nil {
					return map[blizzard.ItemID][]byte{}, err
				}

				// gathering the data from the object
				reader, err := obj.NewReader(phState.IO.StoreClient.Context)
				if err != nil {
					return map[blizzard.ItemID][]byte{}, err
				}
				defer reader.Close()

				out := map[blizzard.ItemID][]byte{}
				r := csv.NewReader(reader)
				for {
					record, err := r.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						return map[blizzard.ItemID][]byte{}, err
					}

					itemIdInt, err := strconv.Atoi(record[0])
					if err != nil {
						return map[blizzard.ItemID][]byte{}, err
					}
					itemId := blizzard.ItemID(itemIdInt)

					base64DecodedPriceHistory, err := base64.StdEncoding.DecodeString(record[1])
					if err != nil {
						return map[blizzard.ItemID][]byte{}, err
					}

					out[itemId] = base64DecodedPriceHistory
				}

				return out, nil
			}()
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to get data")

				continue
			}

			loadInJobs <- database.PricelistHistoryDatabaseEncodedLoadInJob{
				RegionName:                blizzard.RegionName(request.RegionName),
				RealmSlug:                 blizzard.RealmSlug(request.RealmSlug),
				NormalizedTargetTimestamp: sotah.UnixTimestamp(request.NormalizedTargetTimestamp),
				Data: data,
			}
		}
	}
	postWork := func() {
		close(loadInJobs)
	}
	util.Work(8, worker, postWork)

	// queueing it all up
	go func() {
		for _, request := range requests {
			logging.WithFields(logrus.Fields{
				"region": request.RegionName,
				"realm":  request.RealmSlug,
				"normalized-target-timestamp": request.NormalizedTargetTimestamp,
			}).Info("Loading request")

			in <- request
		}

		close(in)
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
	}
}

func (phState ProdPricelistHistoriesState) ListenForComputedLiveAuctions(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			requests, err := database.NewPricelistHistoriesComputeIntakeRequests(busMsg.Data)
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to decode compute-intake requests tuples")

				return
			}

			HandleComputedPricelistHistories(phState, requests)

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
