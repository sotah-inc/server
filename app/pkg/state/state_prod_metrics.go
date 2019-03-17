package state

import (
	"encoding/json"

	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/twinj/uuid"
)

type ProdMetricsStateConfig struct {
	GCloudProjectID string

	MessengerHost string
	MessengerPort int
}

func NewProdMetricsState(config ProdMetricsStateConfig) (ProdMetricsState, error) {
	// establishing an initial state
	metricsState := ProdMetricsState{
		State: NewState(uuid.NewV4(), true),
	}

	// connecting to the messenger host
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return ProdMetricsState{}, err
	}
	metricsState.IO.Messenger = mess

	// establishing a bus
	logging.Info("Connecting bus-client")
	busClient, err := bus.NewClient(config.GCloudProjectID, "prod-metrics")
	if err != nil {
		return ProdMetricsState{}, err
	}
	metricsState.IO.BusClient = busClient

	// initializing a reporter
	metricsState.IO.Reporter = metric.NewReporter(mess)

	// establishing bus-listeners
	metricsState.BusListeners = NewBusListeners(SubjectBusListeners{
		subjects.AppMetrics: metricsState.ListenForMetrics,
	})

	return metricsState, nil
}

type ProdMetricsState struct {
	State
}

func (metricsState ProdMetricsState) ListenForMetrics(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			var m metric.Metrics
			if err := json.Unmarshal([]byte(busMsg.Data), &m); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to marshal metrics")

				return
			}

			metricsState.IO.Reporter.Report(m)
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := metricsState.IO.BusClient.SubscribeToTopic(string(subjects.AppMetrics), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
