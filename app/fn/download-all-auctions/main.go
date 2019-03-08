package auctionscollector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
	"github.com/twinj/uuid"
)

var projectId = os.Getenv("GCP_PROJECT")

var regionRealms map[blizzard.RegionName]sotah.Realms

var busClient bus.Client
var collectAuctionsTopic *pubsub.Topic

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-download-all-auctions")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	collectAuctionsTopic, err = busClient.FirmTopic(string(subjects.DownloadAuctions))
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

type MessageResponses map[string]bus.Message

func (r MessageResponses) IsComplete() bool {
	for _, msg := range r {
		if len(msg.ReplyToId) == 0 {
			return false
		}
	}

	return true
}

func DownloadAllAuctions(_ context.Context, _ PubSubMessage) error {
	// producing a topic and subscribing to receive responses
	downloadedAuctionsResponses := MessageResponses{}
	downloadedAuctionsRecipientTopic, err := busClient.CreateTopic(
		fmt.Sprintf("%s-%s", subjects.DownloadAllAuctions, uuid.NewV4().String()),
	)
	if err != nil {
		return err
	}

	// opening a listener
	onComplete := make(chan interface{})
	receiveDownloadedAuctionsConfig := bus.SubscribeConfig{
		Topic:     downloadedAuctionsRecipientTopic,
		OnReady:   make(chan interface{}),
		Stop:      make(chan interface{}),
		OnStopped: make(chan interface{}),
		Callback: func(busMsg bus.Message) {
			downloadedAuctionsResponses[busMsg.ReplyToId] = busMsg

			if !downloadedAuctionsResponses.IsComplete() {
				return
			}

			onComplete <- struct{}{}
		},
	}
	go func() {
		if err := busClient.Subscribe(receiveDownloadedAuctionsConfig); err != nil {
			log.Fatalf("Failed to subscribe to download-all-auctions recipient topic: %s", err.Error())

			return
		}
	}()
	<-receiveDownloadedAuctionsConfig.OnReady

	// spinning up the workers
	in := make(chan sotah.Realm)
	out := make(chan bus.LoadRegionRealmsOutJob)
	worker := func() {
		for realm := range in {
			job := bus.CollectAuctionsJob{
				RegionName: string(realm.Region.Name),
				RealmSlug:  string(realm.Slug),
			}
			jsonEncoded, err := json.Marshal(job)
			if err != nil {
				out <- bus.LoadRegionRealmsOutJob{
					Err:   err,
					Realm: realm,
				}

				return
			}

			msg := bus.NewMessage()
			msg.Data = string(jsonEncoded)
			msg.ReplyTo = downloadedAuctionsRecipientTopic.ID()
			msg.ReplyToId = fmt.Sprintf("%s-%s", realm.Region.Name, realm.Slug)
			if _, err := busClient.Publish(collectAuctionsTopic, msg); err != nil {
				out <- bus.LoadRegionRealmsOutJob{
					Err:   err,
					Realm: realm,
				}

				return
			}

			out <- bus.LoadRegionRealmsOutJob{
				Err:   nil,
				Realm: realm,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(32, worker, postWork)

	// queueing it up
	go func() {
		for _, realms := range regionRealms {
			for _, realm := range realms {
				in <- realm
			}
		}

		close(in)
	}()

	// waiting for messages to drain out
	for outJob := range out {
		if outJob.Err != nil {
			return err
		}
	}

	// waiting for responses is complete or timer runs out
	timer := time.After(5 * time.Minute)
	select {
	case <-timer:
		return errors.New("timed out")
	case <-onComplete:
		break
	}

	// stopping the downloaded-auctions receiver
	receiveDownloadedAuctionsConfig.Stop <- struct{}{}
	<-receiveDownloadedAuctionsConfig.OnStopped

	// iterating over the results
	for responseId, msg := range downloadedAuctionsResponses {
		logging.WithFields(logrus.Fields{
			"id":  responseId,
			"msg": msg,
		}).Info("Received message")
	}

	return nil
}
