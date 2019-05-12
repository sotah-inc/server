package state

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func ReceiveSyncedItems(itemsState ProdItemsState, idNameMap sotah.ItemIdNameMap) error {
	// declare channels for persisting in
	encodedIn := make(chan database.PersistEncodedItemsInJob)

	// declaring channel for fetching
	getItemsOut := itemsState.ItemsBase.GetItems(idNameMap.ItemIds(), itemsState.ItemsBucket)

	// spinning up a goroutine to multiplex the results between get-items and persist-encoded-items
	go func() {
		for outJob := range getItemsOut {
			if outJob.Err != nil {
				logging.WithFields(outJob.ToLogrusFields()).Error("Failed to fetch item")

				continue
			}

			encodedIn <- database.PersistEncodedItemsInJob{
				Id:              outJob.Id,
				GzipEncodedData: outJob.GzipEncodedData,
			}
		}

		close(encodedIn)
	}()

	return itemsState.IO.Databases.ItemsDatabase.PersistEncodedItems(encodedIn, idNameMap)
}

func (itemsState ProdItemsState) ListenForSyncedItems(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// spinning up a worker
	in := make(chan sotah.ItemIdNameMap, 50)
	go func() {
		for idNameMap := range in {
			// handling item-ids
			logging.WithFields(logrus.Fields{
				"item-ids": len(idNameMap),
				"capacity": len(in),
			}).Info("Received synced item-ids")

			startTime := time.Now()
			if err := ReceiveSyncedItems(itemsState, idNameMap); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to receive synced items")
			}
			logging.WithFields(logrus.Fields{
				"item-ids": len(idNameMap),
				"capacity": len(in),
			}).Info("Done receiving synced item-ids")

			// reporting metrics
			m := metric.Metrics{"receive_synced_items": int(int64(time.Since(startTime)) / 1000 / 1000 / 1000)}
			if err := itemsState.IO.BusClient.PublishMetrics(m); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to publish metric")

				continue
			}
		}
	}()

	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			logging.WithField("subject", subjects.ReceiveSyncedItems).Info("Received message")

			idNormalizedNameMap, err := sotah.NewItemIdNameMap(busMsg.Data)
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to decode item-ids")

				return
			}

			in <- idNormalizedNameMap
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := itemsState.IO.BusClient.SubscribeToTopic(string(subjects.ReceiveSyncedItems), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
