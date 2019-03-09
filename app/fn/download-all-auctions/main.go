package auctionscollector

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

	// producing a topic and subscribing to receive responses
	logging.Info("Producing a topic and subscription to receive responses")
	downloadedAuctionsRecipientTopic, err := busClient.CreateTopic(
		fmt.Sprintf("%s-%s", subjects.DownloadAllAuctions, uuid.NewV4().String()),
	)
	if err != nil {
		return err
	}

	// producing a blank list of message responses
	downloadedAuctionsResponses := MessageResponses{
		Mutex: &sync.Mutex{},
		Items: map[string]bus.Message{},
	}
	for _, realms := range regionRealms {
		for _, realm := range realms {
			downloadedAuctionsResponses.Items[fmt.Sprintf("%s-%s", realm.Region.Name, realm.Slug)] = bus.NewMessage()
		}
	}

	// opening a listener
	logging.Info("Opening a listener and waiting for it to finish opening")
	onComplete := make(chan interface{})
	receiveDownloadedAuctionsConfig := bus.SubscribeConfig{
		Topic:     downloadedAuctionsRecipientTopic,
		OnReady:   make(chan interface{}),
		Stop:      make(chan interface{}),
		OnStopped: make(chan interface{}),
		Callback: func(busMsg bus.Message) {
			downloadedAuctionsResponses.Mutex.Lock()
			defer downloadedAuctionsResponses.Mutex.Unlock()

			downloadedAuctionsResponses.Items[busMsg.ReplyToId] = busMsg

			if !downloadedAuctionsResponses.IsComplete() {
				return
			}

			onComplete <- struct{}{}

			return
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
	logging.Info("Spinning up workers")
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
	startTime := time.Now()
	logging.Info("Queueing it up")
	go func() {
		for _, realms := range regionRealms {
			for _, realm := range realms {
				in <- realm
			}
		}

		close(in)
	}()

	// waiting for messages to drain out
	logging.Info("Waiting for messages to drain out")
	for outJob := range out {
		if outJob.Err != nil {
			return err
		}
	}

	// waiting for responses is complete or timer runs out
	logging.Info("Waiting for responses to complete or timer runs out")
	timer := time.After(200 * time.Second)
	select {
	case <-timer:
	case <-onComplete:
		break
	}
	responseItems := downloadedAuctionsResponses.FilterInCompleted()
	duration := time.Now().Sub(startTime)

	// stopping the downloaded-auctions receiver
	logging.WithFields(
		logrus.Fields{
			"duration":  int(duration.Seconds()),
			"responses": len(responseItems),
		},
	).Info("Finished receiving responses, stopping the listener and waiting for it to stop")
	receiveDownloadedAuctionsConfig.Stop <- struct{}{}
	<-receiveDownloadedAuctionsConfig.OnStopped

	// iterating over the results
	logging.Info("Iterating over the results")
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

		var respData bus.RegionRealmTimestampTuple
		if err := json.Unmarshal([]byte(msg.Data), &respData); err != nil {
			return err
		}

		logging.WithField("resp-data", respData).Info("Received message")
	}

	return nil
}
