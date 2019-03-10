package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/twinj/uuid"
)

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

type CollectAuctionsJob struct {
	RegionName string `json:"region_name"`
	RealmSlug  string `json:"realm_slug"`
}

type RegionRealmTimestampTuple struct {
	RegionName      string `json:"region_name"`
	RealmSlug       string `json:"realm_slug"`
	TargetTimestamp int    `json:"target_timestamp"`
}

type CleanupAuctionManifestJob = RegionRealmTimestampTuple
