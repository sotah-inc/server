package state

import (
	"time"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func ReceiveSyncedItems(itemsState ProdItemsState, ids blizzard.ItemIds) error {
	return nil
}

func (itemsState ProdItemsState) ListenForSyncedItems(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// spinning up a worker
	in := make(chan blizzard.ItemIds, 50)
	go func() {
		for ids := range in {
			// handling item-ids
			logging.WithField("item-ids", len(ids)).Info("Received item-ids")
			startTime := time.Now()
			if err := ReceiveSyncedItems(itemsState, ids); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to receive synced items")
			}
			logging.WithField("item-ids", len(ids)).Info("Done handling item-ids")

			// reporting metrics
			m := metric.Metrics{"receive_synced_items": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000)}
			if err := itemsState.IO.BusClient.PublishMetrics(m); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to publish metric")

				return
			}
		}
	}()

	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			ids, err := blizzard.NewItemIds(busMsg.Data)
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to decode item-ids")

				return
			}

			in <- ids

			return
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
