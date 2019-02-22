package state

import (
	"encoding/json"

	nats "github.com/nats-io/go-nats"
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

func (pubState PubState) ListenForPricelistHistoriesComputeIntake(stop ListenStopChan) error {
	in := make(chan pricelistHistoriesIntakeV2Request, 30)

	err := pubState.IO.Messenger.Subscribe(string(subjects.PricelistHistoriesIntakeV2), stop, func(natsMsg nats.Msg) {
		// resolving the request
		pRequest, err := newPricelistHistoriesIntakeV2Request(natsMsg.Data)
		if err != nil {
			logging.WithField("error", err.Error()).Error("Failed to parse pricelist-histories-intake-request")

			return
		}

		pubState.IO.Reporter.ReportWithPrefix(metric.Metrics{
			"buffer_size": len(pRequest.RegionRealmTimestamps),
		}, kinds.PricelistHistoriesIntakeV2)
		logging.WithField("capacity", len(in)).Info("Received pricelist-histories-intake-v2-request, pushing onto handle channel")

		in <- pRequest
	})
	if err != nil {
		return err
	}

	// starting up a worker to handle pricelist-histories-intake-v2 requests
	go func() {
		for pRequest := range in {
			pRequest.handle(pubState)
		}
	}()

	return nil
}
