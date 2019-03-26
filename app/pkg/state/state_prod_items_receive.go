package state

import (
	"time"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func ReceiveSyncedItems(itemsState ProdItemsState, ids blizzard.ItemIds) error {
	logging.WithField("item-ids", len(ids)).Info("Fetching items")
	items, err := itemsState.ItemsBase.GetItems(ids, itemsState.ItemsBucket)
	if err != nil {
		return err
	}
	itemsToPersist := sotah.ItemsMap{}
	for _, item := range items {
		itemsToPersist[item.ID] = item
	}

	logging.WithField("item-ids", len(ids)).Info("Persisting items")
	return itemsState.IO.Databases.ItemsDatabase.PersistItems(itemsToPersist)
}

func (itemsState ProdItemsState) ListenForSyncedItems(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// spinning up a worker
	in := make(chan blizzard.ItemIds, 50)
	go func() {
		for ids := range in {
			// handling item-ids
			logging.WithField("item-ids", len(ids)).Info("Received synced item-ids")
			startTime := time.Now()
			if err := ReceiveSyncedItems(itemsState, ids); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to receive synced items")
			}
			logging.WithField("item-ids", len(ids)).Info("Done receiving synced item-ids")

			// reporting metrics
			m := metric.Metrics{"receive_synced_items": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000)}
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
