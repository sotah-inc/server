package state

import (
	"time"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func HandleFilterInItemsToSync(busMsg bus.Message, itemsState ProdItemsState, ids blizzard.ItemIds) error {
	toSync, err := itemsState.IO.Databases.ItemsDatabase.FilterInItemsToSync(ids)
	if err != nil {
		return err
	}

	data, err := toSync.EncodeForDelivery()
	if err != nil {
		return err
	}
	reply := bus.NewMessage()
	reply.Data = data

	if _, err := itemsState.IO.BusClient.ReplyTo(busMsg, reply); err != nil {
		return err
	}

	return nil
}

func (itemsState ProdItemsState) ListenForFilterIn(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			ids, err := blizzard.NewItemIds(busMsg.Data)
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to decode item-ids")

				return
			}

			// handling item-ids
			logging.WithField("item-ids", len(ids)).Info("Filtering item-ids")
			startTime := time.Now()
			if err := HandleFilterInItemsToSync(busMsg, itemsState, ids); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to filter in items to sync")
			}
			logging.WithField("item-ids", len(ids)).Info("Done filtering item-ids")

			// reporting metrics
			m := metric.Metrics{"filter_in_items_to_sync": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000)}
			if err := itemsState.IO.BusClient.PublishMetrics(m); err != nil {
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
		if err := itemsState.IO.BusClient.SubscribeToTopic(string(subjects.FilterInItemsToSync), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
