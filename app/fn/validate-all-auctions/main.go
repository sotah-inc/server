package validate_all_auctions

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/sotah-inc/server/app/pkg/metric"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient                         bus.Client
	validateAuctionsTopic             *pubsub.Topic
	computeAllLiveAuctionsTopic       *pubsub.Topic
	computeAllPricelistHistoriesTopic *pubsub.Topic
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-validate-all-auctions")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	validateAuctionsTopic, err = busClient.FirmTopic(string(subjects.ValidateAuctions))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
	computeAllLiveAuctionsTopic, err = busClient.FirmTopic(string(subjects.ComputeAllLiveAuctions))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
	computeAllPricelistHistoriesTopic, err = busClient.FirmTopic(string(subjects.ComputeAllPricelistHistories))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func ValidateAllAuctions(_ context.Context, m PubSubMessage) error {
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
	responseItems, err := busClient.BulkRequest(validateAuctionsTopic, messages, 200*time.Second)
	if err != nil {
		return err
	}

	validatedResponseItems := bus.BulkRequestMessages{}
	for k, msg := range responseItems {
		if msg.Code != codes.Ok {
			logging.WithField("msg", msg).Error("Received erroneous response")

			continue
		}

		validatedResponseItems[k] = msg
	}

	// reporting metrics
	if err := busClient.PublishMetrics(metric.Metrics{
		"validate_all_auctions_duration": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000),
		"included_realms":                len(validatedResponseItems),
	}); err != nil {
		return err
	}

	// formatting the response-items as tuples for processing
	validatedTuples, err := bus.NewRegionRealmTimestampTuplesFromMessages(validatedResponseItems)
	if err != nil {
		return err
	}

	// producing a message for computation
	data, err := validatedTuples.EncodeForDelivery()
	if err != nil {
		return err
	}
	msg := bus.NewMessage()
	msg.Data = data

	// publishing to compute-all-live-auctions
	logging.Info("Publishing to compute-all-live-auctions")
	if _, err := busClient.Publish(computeAllLiveAuctionsTopic, msg); err != nil {
		return err
	}

	// publishing to compute-all-pricelist-histories
	logging.Info("Publishing to compute-all-pricelist-histories")
	if _, err := busClient.Publish(computeAllPricelistHistoriesTopic, msg); err != nil {
		return err
	}

	return nil
}
