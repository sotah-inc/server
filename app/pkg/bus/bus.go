package bus

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
	"github.com/twinj/uuid"
)

func NewCollectAuctionMessages(regionRealms sotah.RegionRealms) ([]Message, error) {
	messages := []Message{}
	for _, realms := range regionRealms {
		for _, realm := range realms {
			job := CollectAuctionsJob{
				RegionName: string(realm.Region.Name),
				RealmSlug:  string(realm.Slug),
			}
			jsonEncoded, err := json.Marshal(job)
			if err != nil {
				return []Message{}, err
			}

			msg := NewMessage()
			msg.Data = string(jsonEncoded)
			msg.ReplyToId = fmt.Sprintf("%s-%s", realm.Region.Name, realm.Slug)
			messages = append(messages, msg)
		}
	}

	return messages, nil
}

func NewItemIdMessages(itemIds blizzard.ItemIds) []Message {
	messages := []Message{}
	for _, id := range itemIds {
		msg := NewMessage()
		msg.Data = strconv.Itoa(int(id))
		msg.ReplyToId = fmt.Sprintf("item-%d", id)
		messages = append(messages, msg)
	}

	return messages
}

func NewMessage() Message {
	return Message{Code: codes.Ok}
}

type Message struct {
	Data      string     `json:"data"`
	Err       string     `json:"error"`
	Code      codes.Code `json:"code"`
	ReplyTo   string     `json:"reply_to"`
	ReplyToId string     `json:"reply_to_id"`
}

func NewClient(projectID string, subscriberId string) (Client, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return Client{}, err
	}

	return Client{
		client:       client,
		context:      ctx,
		projectId:    projectID,
		subscriberId: subscriberId,
	}, nil
}

type Client struct {
	context      context.Context
	projectId    string
	client       *pubsub.Client
	subscriberId string
}

func (c Client) CreateTopic(id string) (*pubsub.Topic, error) {
	return c.client.CreateTopic(c.context, id)
}

func (c Client) Topic(topicName string) *pubsub.Topic {
	return c.client.Topic(topicName)
}

func (c Client) FirmTopic(topicName string) (*pubsub.Topic, error) {
	topic := c.Topic(topicName)

	exists, err := topic.Exists(c.context)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, errors.New("topic does not exist")
	}

	return topic, nil
}

func (c Client) ResolveTopic(id string) (*pubsub.Topic, error) {
	topic := c.Topic(id)
	exists, err := topic.Exists(c.context)
	if err != nil {
		return nil, err
	}
	if exists {
		return topic, nil
	}

	return c.CreateTopic(id)
}

func (c Client) CreateSubscription(topic *pubsub.Topic) (*pubsub.Subscription, error) {
	return c.client.CreateSubscription(c.context, c.subscriberName(topic), pubsub.SubscriptionConfig{
		Topic: topic,
	})
}

func (c Client) Publish(topic *pubsub.Topic, msg Message) (string, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}

	return topic.Publish(c.context, &pubsub.Message{Data: data}).Get(c.context)
}

type BulkPublishOutJob struct {
	Err error
	Msg Message
}

func (c Client) BulkPublish(topic *pubsub.Topic, messages []Message) chan BulkPublishOutJob {
	// opening workers and channels
	in := make(chan Message)
	out := make(chan BulkPublishOutJob)
	worker := func() {
		for msg := range in {
			if _, err := c.Publish(topic, msg); err != nil {
				out <- BulkPublishOutJob{
					Err: err,
					Msg: msg,
				}

				continue
			}

			out <- BulkPublishOutJob{
				Err: nil,
				Msg: msg,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(32, worker, postWork)

	// queueing it up
	go func() {
		for _, msg := range messages {
			in <- msg
		}

		close(in)
	}()

	return out
}

func (c Client) subscriberName(topic *pubsub.Topic) string {
	return fmt.Sprintf("subscriber-%s-%s-%s", c.subscriberId, topic.ID(), uuid.NewV4().String())
}

func (c Client) SubscribeToTopic(id string, config SubscribeConfig) error {
	topic, err := c.ResolveTopic(id)
	if err != nil {
		return err
	}
	config.Topic = topic

	return c.Subscribe(config)
}

type SubscribeConfig struct {
	Topic     *pubsub.Topic
	Stop      chan interface{}
	OnReady   chan interface{}
	OnStopped chan interface{}
	Callback  func(Message)
}

func (c Client) Subscribe(config SubscribeConfig) error {
	sub, err := c.CreateSubscription(config.Topic)
	if err != nil {
		return err
	}

	config.OnReady <- struct{}{}

	entry := logging.WithFields(logrus.Fields{
		"subscriber-name": sub.ID(),
		"topic":           config.Topic.ID(),
	})

	cctx, cancel := context.WithCancel(c.context)
	go func() {
		<-config.Stop

		cancel()
		config.Topic.Stop()

		config.OnStopped <- struct{}{}
	}()

	entry.Info("Waiting for messages")
	err = sub.Receive(cctx, func(ctx context.Context, pubsubMsg *pubsub.Message) {
		pubsubMsg.Ack()

		var msg Message
		if err := json.Unmarshal(pubsubMsg.Data, &msg); err != nil {
			entry.WithField("error", err.Error()).Error("Failed to parse message")

			return
		}

		config.Callback(msg)
	})
	if err != nil {
		if err == context.Canceled {
			return nil
		}

		return err
	}

	return nil
}

func (c Client) ReplyTo(target Message, payload Message) (string, error) {
	if target.ReplyTo == "" {
		return "", errors.New("cannot reply to blank reply-to topic name")
	}

	// validating topic already exists
	topic, err := c.FirmTopic(target.ReplyTo)
	if err != nil {
		return "", err
	}

	logging.WithField("reply-to-topic", topic.ID()).Info("Replying to topic")

	return c.Publish(topic, payload)
}

func (c Client) RequestFromTopic(topicName string, payload string, timeout time.Duration) (Message, error) {
	topic, err := c.FirmTopic(topicName)
	if err != nil {
		return Message{}, err
	}

	return c.Request(topic, payload, timeout)
}

type MessageResponses struct {
	Items BulkRequestMessages
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

func (r MessageResponses) FilterInCompleted() BulkRequestMessages {
	out := BulkRequestMessages{}
	for k, v := range r.Items {
		if len(v.ReplyToId) == 0 {
			continue
		}

		out[k] = v
	}

	return out
}

func NewBulkRequestMessages(messages []Message) BulkRequestMessages {
	out := BulkRequestMessages{}
	for _, msg := range messages {
		out[msg.ReplyToId] = NewMessage()
	}

	return out
}

type BulkRequestMessages map[string]Message

func (c Client) BulkRequest(intakeTopic *pubsub.Topic, messages []Message, timeout time.Duration) (BulkRequestMessages, error) {
	// producing a topic to receive responses
	logging.Info("Producing a topic and subscription to receive responses")
	recipientTopic, err := c.CreateTopic(fmt.Sprintf("bulk-request-%s", uuid.NewV4().String()))
	if err != nil {
		return BulkRequestMessages{}, err
	}

	// updating messages with reply-to topic
	for i, msg := range messages {
		msg.ReplyTo = recipientTopic.ID()
		messages[i] = msg
	}

	// producing a blank list of message responses
	responses := MessageResponses{
		Mutex: &sync.Mutex{},
		Items: NewBulkRequestMessages(messages),
	}

	// opening a listener
	logging.Info("Opening a listener and waiting for it to finish opening")
	onComplete := make(chan interface{})
	receiveConfig := SubscribeConfig{
		Topic:     recipientTopic,
		OnReady:   make(chan interface{}),
		Stop:      make(chan interface{}),
		OnStopped: make(chan interface{}),
		Callback: func(busMsg Message) {
			responses.Mutex.Lock()
			defer responses.Mutex.Unlock()
			responses.Items[busMsg.ReplyToId] = busMsg

			if !responses.IsComplete() {
				return
			}

			onComplete <- struct{}{}

			return
		},
	}
	go func() {
		if err := c.Subscribe(receiveConfig); err != nil {
			logging.Fatalf("Failed to subscribe to recipient topic: %s", err.Error())

			return
		}
	}()
	<-receiveConfig.OnReady

	// bulk publishing
	logging.Info("Bulk publishing")
	startTime := time.Now()
	for outJob := range c.BulkPublish(intakeTopic, messages) {
		if outJob.Err != nil {
			return BulkRequestMessages{}, outJob.Err
		}
	}

	// waiting for responses is complete or timer runs out
	logging.Info("Waiting for responses to complete or timer runs out")
	timer := time.After(timeout)
	select {
	case <-timer:
		logging.Info("Timer timed out, going over results in allotted time")

		break
	case <-onComplete:
		logging.Info("Received all responses, going over all responses")

		break
	}
	responseItems := responses.FilterInCompleted()
	duration := time.Now().Sub(startTime)

	// stopping the receiver
	logging.WithFields(
		logrus.Fields{
			"duration":  int(duration.Seconds()),
			"responses": len(responseItems),
		},
	).Info("Finished receiving responses, stopping the listener and waiting for it to stop")
	receiveConfig.Stop <- struct{}{}
	<-receiveConfig.OnStopped

	return responses.FilterInCompleted(), nil
}

type requestJob struct {
	Err     error
	Payload Message
}

func (c Client) Request(recipientTopic *pubsub.Topic, payload string, timeout time.Duration) (Message, error) {
	// producing a reply-to topic
	replyToTopic, err := c.client.CreateTopic(c.context, fmt.Sprintf("reply-to-%s", uuid.NewV4().String()))
	if err != nil {
		return Message{}, err
	}

	// producing a reply-to subscription
	replyToSub, err := c.client.CreateSubscription(c.context, c.subscriberName(replyToTopic), pubsub.SubscriptionConfig{
		Topic: replyToTopic,
	})
	if err != nil {
		return Message{}, err
	}

	cctx, cancel := context.WithCancel(c.context)

	// spawning a worker to wait for a response on the reply-to topic
	out := make(chan requestJob)
	go func() {
		// spawning a receiver worker to receive the results and push them out
		receiver := make(chan requestJob)
		go func() {
			select {
			case result := <-receiver:
				close(receiver)

				cancel()
				if err := replyToSub.Delete(c.context); err != nil {
					logging.WithFields(logrus.Fields{
						"error":        err.Error(),
						"subscription": replyToSub.ID(),
					}).Error("Failed to delete reply-to subscription after receiving result")

					out <- requestJob{
						Err:     err,
						Payload: Message{},
					}

					return
				}

				replyToTopic.Stop()
				if err := replyToTopic.Delete(c.context); err != nil {
					logging.WithFields(logrus.Fields{
						"error": err.Error(),
						"topic": replyToTopic.ID(),
					}).Error("Failed to delete reply-to topic after receiving result")

					return
				}

				out <- result

				return
			case <-time.After(timeout):
				close(receiver)

				cancel()
				if err := replyToSub.Delete(c.context); err != nil {
					logging.WithFields(logrus.Fields{
						"error":        err.Error(),
						"subscription": replyToSub.ID(),
					}).Error("Failed to delete reply-to subscription after timing out")

					out <- requestJob{
						Err:     err,
						Payload: Message{},
					}

					return
				}

				replyToTopic.Stop()
				if err := replyToTopic.Delete(c.context); err != nil {
					logging.WithFields(logrus.Fields{
						"error": err.Error(),
						"topic": replyToTopic.ID(),
					}).Error("Failed to delete reply-to topic after timing out")

					out <- requestJob{
						Err:     err,
						Payload: Message{},
					}

					return
				}

				out <- requestJob{
					Err:     errors.New("timed out"),
					Payload: Message{},
				}

				return
			}
		}()

		// waiting for a message to come through
		err = replyToSub.Receive(cctx, func(ctx context.Context, pubsubMsg *pubsub.Message) {
			pubsubMsg.Ack()

			var msg Message
			if err := json.Unmarshal(pubsubMsg.Data, &msg); err != nil {
				receiver <- requestJob{
					Err:     err,
					Payload: Message{},
				}

				return
			}

			receiver <- requestJob{
				Err:     nil,
				Payload: msg,
			}
		})
		if err != nil {
			if err == context.Canceled {
				return
			}

			close(receiver)
			cancel()
			replyToTopic.Stop()

			out <- requestJob{
				Err:     err,
				Payload: Message{},
			}

			return
		}
	}()

	// publishing the payload to the recipient topic
	msg := NewMessage()
	msg.Data = payload
	msg.ReplyTo = replyToTopic.ID()
	jsonEncodedMessage, err := json.Marshal(msg)
	if err != nil {
		close(out)

		return Message{}, err
	}

	if _, err := recipientTopic.Publish(c.context, &pubsub.Message{Data: jsonEncodedMessage}).Get(c.context); err != nil {
		close(out)

		return Message{}, err
	}

	// waiting for a result to come out
	requestResult := <-out

	close(out)

	if requestResult.Err != nil {
		return Message{}, requestResult.Err
	}

	return requestResult.Payload, nil
}

func (c Client) PublishMetrics(m metric.Metrics) error {
	topic, err := c.FirmTopic(string(subjects.AppMetrics))
	if err != nil {
		return err
	}

	jsonEncoded, err := json.Marshal(m)
	if err != nil {
		return err
	}

	msg := NewMessage()
	msg.Data = string(jsonEncoded)
	if _, err := c.Publish(topic, msg); err != nil {
		return err
	}

	return nil
}

type CollectAuctionsJob struct {
	RegionName string `json:"region_name"`
	RealmSlug  string `json:"realm_slug"`
}

func NewRegionRealmTimestampTuplesFromMessages(messages BulkRequestMessages) (RegionRealmTimestampTuples, error) {
	tuples := RegionRealmTimestampTuples{}
	for _, msg := range messages {
		var respData RegionRealmTimestampTuple
		if err := json.Unmarshal([]byte(msg.Data), &respData); err != nil {
			return RegionRealmTimestampTuples{}, err
		}

		tuples = append(tuples, respData)
	}

	return tuples, nil
}

func NewItemIdsFromMessages(messages BulkRequestMessages) (blizzard.ItemIds, error) {
	itemIdsMap := map[blizzard.ItemID]interface{}{}
	for _, msg := range messages {
		itemIds, err := blizzard.NewItemIds(msg.Data)
		if err != nil {
			return blizzard.ItemIds{}, err
		}

		for _, id := range itemIds {
			itemIdsMap[id] = struct{}{}
		}
	}

	out := blizzard.ItemIds{}
	for id := range itemIdsMap {
		out = append(out, id)
	}

	return out, nil
}

func NewPricelistHistoriesComputeIntakeRequestsFromMessages(
	messages BulkRequestMessages,
) (database.PricelistHistoriesComputeIntakeRequests, error) {
	out := database.PricelistHistoriesComputeIntakeRequests{}
	for _, msg := range messages {
		var respData database.PricelistHistoriesComputeIntakeRequest
		if err := json.Unmarshal([]byte(msg.Data), &respData); err != nil {
			return database.PricelistHistoriesComputeIntakeRequests{}, err
		}

		out = append(out, respData)
	}

	return out, nil
}

func NewRegionRealmTimestampTuples(data string) (RegionRealmTimestampTuples, error) {
	base64Decoded, err := base64.RawStdEncoding.DecodeString(data)
	if err != nil {
		return RegionRealmTimestampTuples{}, err
	}

	gzipDecoded, err := util.GzipDecode(base64Decoded)
	if err != nil {
		return RegionRealmTimestampTuples{}, err
	}

	var out RegionRealmTimestampTuples
	if err := json.Unmarshal(gzipDecoded, &out); err != nil {
		return RegionRealmTimestampTuples{}, err
	}

	return out, nil
}

type RegionRealmTimestampTuples []RegionRealmTimestampTuple

func (s RegionRealmTimestampTuples) EncodeForDelivery() (string, error) {
	jsonEncoded, err := json.Marshal(s)
	if err != nil {
		return "", err
	}

	gzipEncoded, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return "", err
	}

	return base64.RawStdEncoding.EncodeToString(gzipEncoded), nil
}

func (s RegionRealmTimestampTuples) ToMessages() ([]Message, error) {
	out := []Message{}
	for _, tuple := range s {
		msg := NewMessage()
		msg.ReplyToId = fmt.Sprintf("%s-%s", tuple.RegionName, tuple.RealmSlug)

		job := LoadRegionRealmTimestampsInJob{
			RegionName:      tuple.RegionName,
			RealmSlug:       tuple.RealmSlug,
			TargetTimestamp: tuple.TargetTimestamp,
		}
		data, err := job.EncodeForDelivery()
		if err != nil {
			return []Message{}, err
		}
		msg.Data = data

		out = append(out, msg)
	}

	return out, nil
}

type RegionRealmTimestampTuple struct {
	RegionName                string   `json:"region_name"`
	RealmSlug                 string   `json:"realm_slug"`
	TargetTimestamp           int      `json:"target_timestamp"`
	NormalizedTargetTimestamp int      `json:"normalized_target_timestamp"`
	ItemIds                   []int    `json:"item_ids"`
	OwnerNames                []string `json:"owner_names"`
}

func (t RegionRealmTimestampTuple) EncodeForDelivery() (string, error) {
	data, err := json.Marshal(t)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

type CleanupAuctionManifestJob = RegionRealmTimestampTuple
