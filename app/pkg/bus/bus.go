package bus

import (
	"context"

	"cloud.google.com/go/pubsub"
)

func NewBus(projectID string) (Bus, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return Bus{}, err
	}

	return Bus{
		client:    client,
		context:   ctx,
		projectId: projectID,
	}, nil
}

type Bus struct {
	context   context.Context
	projectId string
	client    *pubsub.Client
}

func (b Bus) Subscribe(subscriberName string, topicName string, stop chan interface{}, cb func(pubsub.Message)) error {
	topic, err := b.client.CreateTopic(b.context, topicName)
	if err != nil {
		return err
	}

	sub, err := b.client.CreateSubscription(b.context, subscriberName, pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(b.context)
	go func() {
		<-stop

		cancel()
	}()

	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()

		cb(*msg)
	})
	if err != nil {
		if err == context.Canceled {
			return nil
		}

		return err
	}

	return nil
}
