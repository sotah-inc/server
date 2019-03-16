package download_all_auctions

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	regionRealms map[blizzard.RegionName]sotah.Realms

	busClient                bus.Client
	downloadAuctionsTopic    *pubsub.Topic
	validateAllAuctionsTopic *pubsub.Topic
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-download-all-auctions")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	downloadAuctionsTopic, err = busClient.FirmTopic(string(subjects.DownloadAuctions))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
	validateAllAuctionsTopic, err = busClient.FirmTopic(string(subjects.ValidateAllAuctions))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}

	storeClient, err := store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}

	bootBase := store.NewBootBase(storeClient, "us-central1")
	var bootBucket *storage.BucketHandle
	bootBucket, err = bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}
	regionRealms, err = bootBase.GetRegionRealms(bootBucket)
	if err != nil {
		log.Fatalf("Failed to get region-realms: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func DownloadAllAuctions(_ context.Context, m PubSubMessage) error {
	if len(m.Data) == 0 {
		return errors.New("fail")
	}

	// producing messages
	logging.Info("Producing messages for bulk requesting")
	messages, err := bus.NewCollectAuctionMessages(regionRealms)
	if err != nil {
		return err
	}

	// enqueueing them and gathering result jobs
	responseItems, err := busClient.BulkRequest(downloadAuctionsTopic, messages, 200*time.Second)
	if err != nil {
		return err
	}

	validatedResponseItems := bus.BulkRequestMessages{}
	for k, msg := range responseItems {
		if msg.Code != codes.Ok {
			if msg.Code == codes.BlizzardError {
				var respError blizzard.ResponseError
				if err := json.Unmarshal([]byte(msg.Data), &respError); err != nil {
					return err
				}

				logging.WithFields(logrus.Fields{"resp-error": respError}).Error("Received erroneous response")
			}

			continue
		}

		// ok msg code but no msg data means no new auctions
		if len(msg.Data) == 0 {
			continue
		}

		validatedResponseItems[k] = msg
	}

	// formatting the response-items as tuples for processing
	tuples, err := bus.NewRegionRealmTimestampTuplesFromMessages(validatedResponseItems)
	if err != nil {
		return err
	}

	// producing a message for computation
	data, err := tuples.EncodeForDelivery()
	if err != nil {
		return err
	}
	msg := bus.NewMessage()
	msg.Data = data

	// publishing to validate-all-auctions
	if _, err := busClient.Publish(validateAllAuctionsTopic, msg); err != nil {
		return err
	}

	return nil
}
