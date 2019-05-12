package state

import (
	"errors"
	"io/ioutil"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
)

func HandleComputedLiveAuctions(liveAuctionsState ProdLiveAuctionsState, tuples bus.RegionRealmTimestampTuples) {
	// declaring a load-in channel for the live-auctions db and starting it up
	loadInJobs := make(chan database.LiveAuctionsLoadEncodedDataInJob)
	loadOutJobs := liveAuctionsState.IO.Databases.LiveAuctionsDatabases.LoadEncodedData(loadInJobs)

	// starting workers for handling tuples
	in := make(chan bus.RegionRealmTimestampTuple)
	worker := func() {
		for tuple := range in {
			// resolving the realm from the request
			realm, err := func() (sotah.Realm, error) {
				for regionName, status := range liveAuctionsState.Statuses {
					if regionName != blizzard.RegionName(tuple.RegionName) {
						continue
					}

					for _, realm := range status.Realms {
						if realm.Slug != blizzard.RealmSlug(tuple.RealmSlug) {
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
			data, err := func() ([]byte, error) {
				obj, err := liveAuctionsState.LiveAuctionsBase.GetFirmObject(realm, liveAuctionsState.LiveAuctionsBucket)
				if err != nil {
					return []byte{}, err
				}

				reader, err := obj.ReadCompressed(true).NewReader(liveAuctionsState.IO.StoreClient.Context)
				if err != nil {
					return []byte{}, err
				}
				defer reader.Close()

				return ioutil.ReadAll(reader)
			}()
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to get data")

				continue
			}

			loadInJobs <- database.LiveAuctionsLoadEncodedDataInJob{
				RegionName:  blizzard.RegionName(tuple.RegionName),
				RealmSlug:   blizzard.RealmSlug(tuple.RealmSlug),
				EncodedData: data,
			}
		}
	}
	postWork := func() {
		close(loadInJobs)
	}
	util.Work(4, worker, postWork)

	// queueing it all up
	go func() {
		for _, tuple := range tuples {
			logging.WithFields(logrus.Fields{
				"region": tuple.RegionName,
				"realm":  tuple.RealmSlug,
			}).Info("Loading tuple")

			in <- tuple
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

func (liveAuctionsState ProdLiveAuctionsState) ListenForComputedLiveAuctions(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			tuples, err := bus.NewRegionRealmTimestampTuples(busMsg.Data)
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to decode region-realm-timestamps tuples")

				return
			}

			// handling requests
			logging.WithField("requests", len(tuples)).Info("Received tuples")
			startTime := time.Now()
			HandleComputedLiveAuctions(liveAuctionsState, tuples)
			logging.WithField("requests", len(tuples)).Info("Done handling tuples")

			// reporting metrics
			m := metric.Metrics{"receive_all_live_auctions_duration": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000)}
			if err := liveAuctionsState.IO.BusClient.PublishMetrics(m); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to publish metric")

				return
			}
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := liveAuctionsState.IO.BusClient.SubscribeToTopic(string(subjects.ReceiveComputedLiveAuctions), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
