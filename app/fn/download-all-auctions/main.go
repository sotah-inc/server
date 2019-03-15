package download_all_auctions

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
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

	bootResponse, err := func() (state.AuthenticatedBootResponse, error) {
		msg, err := busClient.RequestFromTopic(string(subjects.Boot), "", 5*time.Second)
		if err != nil {
			return state.AuthenticatedBootResponse{}, err
		}

		var out state.AuthenticatedBootResponse
		if err := json.Unmarshal([]byte(msg.Data), &out); err != nil {
			return state.AuthenticatedBootResponse{}, err
		}

		return out, nil
	}()
	if err != nil {
		log.Fatalf("Failed to get authenticated-boot-response: %s", err.Error())

		return
	}

	regions := bootResponse.Regions

	regionRealms = map[blizzard.RegionName]sotah.Realms{}
	for job := range busClient.LoadStatuses(regions) {
		if job.Err != nil {
			log.Fatalf("Failed to fetch status: %s", job.Err.Error())

			return
		}

		regionRealms[job.Region.Name] = job.Status.Realms
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
