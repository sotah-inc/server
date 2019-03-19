package compute_all_pricelist_histories

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient                               bus.Client
	computePricelistHistoriesTopic          *pubsub.Topic
	receivedComputedPricelistHistoriesTopic *pubsub.Topic
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-compute-all-pricelist-histories")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	computePricelistHistoriesTopic, err = busClient.FirmTopic(string(subjects.ComputePricelistHistories))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
	receivedComputedPricelistHistoriesTopic, err = busClient.FirmTopic(string(subjects.ReceiveComputedPricelistHistories))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func ComputeAllPricelistHistories(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	// producing region-realm-timestamp tuples from bus message
	tuples, err := bus.NewRegionRealmTimestampTuples(in.Data)
	if err != nil {
		return err
	}

	// converting to messages for requesting
	messages, err := tuples.ToMessages()
	if err != nil {
		return err
	}

	// enqueueing them and gathering result jobs
	startTime := time.Now()
	responseItems, err := busClient.BulkRequest(computePricelistHistoriesTopic, messages, 500*time.Second)
	if err != nil {
		return err
	}

	// reporting metrics
	met := metric.Metrics{"compute_all_pricelist_histories_duration": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000)}
	if err := busClient.PublishMetrics(met); err != nil {
		return err
	}

	validatedResponseItems := bus.BulkRequestMessages{}
	for k, msg := range responseItems {
		if msg.Code != codes.Ok {
			logging.WithField("msg", msg).Error("Received erroneous response")

			continue
		}

		validatedResponseItems[k] = msg

		break
	}

	// formatting the response-items as requests for processing
	requests, err := bus.NewPricelistHistoriesComputeIntakeRequestsFromMessages(validatedResponseItems)
	if err != nil {
		return err
	}

	// producing a message for computation
	data, err := requests.EncodeForDelivery()
	if err != nil {
		return err
	}
	msg := bus.NewMessage()
	msg.Data = data

	// publishing to receive-computed-pricelist-histories
	logging.Info("Publishing to receive-computed-pricelist-histories")
	if _, err := busClient.Publish(receivedComputedPricelistHistoriesTopic, msg); err != nil {
		return err
	}

	return nil
}
