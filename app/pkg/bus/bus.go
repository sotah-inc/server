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
	Data    string     `json:"data"`
	Err     string     `json:"error"`
	Code    codes.Code `json:"code"`
	ReplyTo string     `json:"reply_to"`
}

func NewBus(projectID string, subscriberId string) (Bus, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return Bus{}, err
	}

	return Bus{
		client:       client,
		context:      ctx,
		projectId:    projectID,
		subscriberId: subscriberId,
	}, nil
}

type Bus struct {
	context      context.Context
	projectId    string
	client       *pubsub.Client
	subscriberId string
}

func (b Bus) ResolveTopic(topicName string) (*pubsub.Topic, error) {
	topic := b.client.Topic(topicName)
	exists, err := topic.Exists(b.context)
	if err != nil {
		return nil, err
	}

	if exists {
		return topic, nil
	}

	return b.client.CreateTopic(b.context, topicName)
}

func (b Bus) resolveSubscription(topic *pubsub.Topic, subscriberName string) (*pubsub.Subscription, error) {
	subscription := b.client.Subscription(subscriberName)
	exists, err := subscription.Exists(b.context)
	if err != nil {
		return nil, err
	}

	if exists {
		return subscription, nil
	}

	return b.client.CreateSubscription(b.context, subscriberName, pubsub.SubscriptionConfig{Topic: topic})
}

func (b Bus) PublishToTopic(topicName string, msg Message) (string, error) {
	topic, err := b.ResolveTopic(topicName)
	if err != nil {
		return "", err
	}

	return b.Publish(topic, msg)
}

func (b Bus) Publish(topic *pubsub.Topic, msg Message) (string, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}

	return topic.Publish(b.context, &pubsub.Message{Data: data}).Get(b.context)
}

func (b Bus) SubscribeToTopic(topicName string, stop chan interface{}, cb func(Message)) error {
	topic, err := b.ResolveTopic(topicName)
	if err != nil {
		return err
	}

	return b.Subscribe(topic, stop, cb)
}

func (b Bus) Subscribe(topic *pubsub.Topic, stop chan interface{}, cb func(Message)) error {
	subscriberName := fmt.Sprintf("subscriber-%s", b.subscriberId)

	entry := logging.WithFields(logrus.Fields{
		"subscriber-name": subscriberName,
		"topic":           topic.ID(),
	})

	entry.Info("Subscribing to topic")
	sub, err := b.client.CreateSubscription(b.context, subscriberName, pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(b.context)
	go func() {
		<-stop

		entry.Info("Received stop signal, cancelling subscription")

		cancel()

		entry.Info("Stopping topic")
		topic.Stop()
	}()

	entry.Info("Waiting for messages")
	err = sub.Receive(cctx, func(ctx context.Context, pubsubMsg *pubsub.Message) {
		pubsubMsg.Ack()

		var msg Message
		if err := json.Unmarshal(pubsubMsg.Data, &msg); err != nil {
			entry.WithField("error", err.Error()).Error("Failed to parse message")

			return
		}

		cb(msg)
	})
	if err != nil {
		if err == context.Canceled {
			return nil
		}

		return err
	}

	return nil
}

func (b Bus) ReplyTo(target Message, payload Message) (string, error) {
	if target.ReplyTo == "" {
		return "", errors.New("cannot reply to blank reply-to topic name")
	}

	return b.PublishToTopic(target.ReplyTo, payload)
}

func (b Bus) RequestFromTopic(topicName string, payload string, timeout time.Duration) (Message, error) {
	topic, err := b.ResolveTopic(topicName)
	if err != nil {
		return Message{}, err
	}

	return b.Request(topic, payload, timeout)
}

type requestJob struct {
	Err     error
	Payload Message
}

func (b Bus) Request(recipientTopic *pubsub.Topic, payload string, timeout time.Duration) (Message, error) {
	// producing a reply-to topic
	replyToTopic, err := b.ResolveTopic(fmt.Sprintf("reply-to-%s", uuid.NewV4().String()))
	if err != nil {
		return Message{}, err
	}

	entry := logging.WithFields(logrus.Fields{
		"recipient-topic": recipientTopic.ID(),
		"reply-to-topic":  replyToTopic.ID(),
	})

	// spawning a worker to wait for a response on the reply-to topic
	entry.Debug("Spawning worker to wait for response on reply-to topic")
	out := make(chan requestJob)
	go func() {
		stop := make(chan interface{})

		// spawning a receiver worker to receive the results and push them out
		receiver := make(chan requestJob)
		go func() {
			select {
			case result := <-receiver:
				close(receiver)

				entry.Debug("Received reply message on receiver, sending to out channel")
				out <- result

				// sending a signal to close the subscriber
				entry.Debug("Sending stop signal to reply-to subscription and channel")
				stop <- struct{}{}
			case <-time.After(timeout):
				close(receiver)

				entry.Debug("Did not receive reply on reply-to topic within timeout period, sending timed out error to out channel")
				out <- requestJob{
					Err:     errors.New("timed out"),
					Payload: Message{},
				}

				entry.Debug("Sending stop signal to reply-to subscription and channel")
				stop <- struct{}{}
			}
		}()

		// waiting for a message to come through
		err := b.Subscribe(replyToTopic, stop, func(msg Message) {
			entry.Debug("Received reply message on reply-to topic, forwarding to receiver")

			receiver <- requestJob{
				Err:     nil,
				Payload: msg,
			}
		})
		if err != nil {
			entry.WithField("error", err.Error()).Error("Failed to subscribe to reply-to topic, dumping error to out channel")

			close(receiver)
			out <- requestJob{
				Err:     err,
				Payload: Message{},
			}

			return
		}
	}()

	// publishing the payload to the reply-to topic
	msg := NewMessage()
	msg.Data = payload
	msg.ReplyTo = replyToTopic.ID()
	jsonEncodedMessage, err := json.Marshal(msg)
	if err != nil {
		close(out)

		return Message{}, err
	}

	entry.Debug("Sending message to recipient topic")
	if _, err := recipientTopic.Publish(b.context, &pubsub.Message{Data: jsonEncodedMessage}).Get(b.context); err != nil {
		close(out)

		return Message{}, err
	}

	// waiting for a result to come out
	entry.Debug("Waiting for result to come out of reply-to topic")
	requestResult := <-out

	entry.Debug("Received response in reply-to topic, closing out channel")
	close(out)

	if requestResult.Err != nil {
		entry.WithField("error", requestResult.Err.Error()).Error("Received error on out channel")

		return Message{}, requestResult.Err
	}

	entry.Debug("Successfully received response on reply-to topic")

	return requestResult.Payload, nil
}
