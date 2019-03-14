package download_all_auctions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
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

var projectId = os.Getenv("GCP_PROJECT")

var regionRealms map[blizzard.RegionName]sotah.Realms

var busClient bus.Client
var downloadAuctionsTopic *pubsub.Topic
var computeAllLiveAuctionsTopic *pubsub.Topic

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
	computeAllLiveAuctionsTopic, err = busClient.FirmTopic(string(subjects.ComputeAllLiveAuctions))
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

type MessageResponses struct {
	Items map[string]bus.Message
	Mutex *sync.Mutex
}

func (r MessageResponses) IsComplete() bool {
	for _, msg := range r.Items {
		if len(msg.ReplyToId) == 0 {
			return false
		}
	}

	return true
}

func (r MessageResponses) FilterInCompleted() map[string]bus.Message {
	out := map[string]bus.Message{}
	for k, v := range r.Items {
		if len(v.ReplyToId) == 0 {
			continue
		}

		out[k] = v
	}

	return out
}

func DownloadAllAuctions(_ context.Context, m PubSubMessage) error {
	if len(m.Data) == 0 {
		return errors.New("fail")
	}

	// producing messages
	logging.Info("Producing messages for bulk requestinggg")
	messages := []bus.Message{}
	for _, realms := range regionRealms {
		for _, realm := range realms {
			job := bus.CollectAuctionsJob{
				RegionName: string(realm.Region.Name),
				RealmSlug:  string(realm.Slug),
			}
			jsonEncoded, err := json.Marshal(job)
			if err != nil {
				return err
			}

			msg := bus.NewMessage()
			msg.Data = string(jsonEncoded)
			msg.ReplyToId = fmt.Sprintf("%s-%s", realm.Region.Name, realm.Slug)
			messages = append(messages, msg)
		}
	}

	// enqueueing them and gathering result jobs
	responseItems, err := busClient.BulkRequest(downloadAuctionsTopic, messages, 200*time.Second)
	if err != nil {
		return err
	}

	// formatting the response-items as tuples for processing
	tuples := bus.RegionRealmTimestampTuples{}
	for _, msg := range responseItems {
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

		var respData bus.RegionRealmTimestampTuple
		if err := json.Unmarshal([]byte(msg.Data), &respData); err != nil {
			return err
		}

		tuples = append(tuples, respData)
	}

	// producing a message for computation
	data, err := tuples.EncodeForDelivery()
	if err != nil {
		return err
	}
	msg := bus.NewMessage()
	msg.Data = data
	if _, err := busClient.Publish(computeAllLiveAuctionsTopic, msg); err != nil {
		return err
	}

	return nil
}
