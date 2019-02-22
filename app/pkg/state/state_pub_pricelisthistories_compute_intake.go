package state

import (
	"encoding/json"

	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/metric/kinds"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func newPricelistHistoriesComputeIntakeRequest(data []byte) (pricelistHistoriesComputeIntakeRequest, error) {
	pRequest := &pricelistHistoriesComputeIntakeRequest{}
	err := json.Unmarshal(data, &pRequest)
	if err != nil {
		return pricelistHistoriesComputeIntakeRequest{}, err
	}

	return *pRequest, nil
}

type pricelistHistoriesComputeIntakeRequest struct {
	RegionName                string `json:"region_name"`
	RealmSlug                 string `json:"realm_slug"`
	NormalizedTargetTimestamp int    `json:"normalized_target_timestamp"`
}

func (pRequest pricelistHistoriesComputeIntakeRequest) handle(pubState PubState) {
	return
}

func (pubState PubState) ListenForPricelistHistoriesComputeIntake(stop ListenStopChan, onReady chan interface{}, onStopped chan interface{}) error {
	in := make(chan pricelistHistoriesComputeIntakeRequest, 30)

	topic, err := pubState.IO.BusClient.ResolveTopic(string(subjects.PricelistHistoriesComputeIntake))
	if err != nil {
		return err
	}

	config := bus.SubscribeConfig{
		Stop:  stop,
		Topic: topic,
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
	if err := pubState.IO.BusClient.Subscribe(config); err != nil {
		return err
	}

	// starting up a worker to handle pricelist-histories-compute-intake requests
	go func() {
		for pRequest := range in {
			pRequest.handle(pubState)
		}
	}()

	return nil
}
