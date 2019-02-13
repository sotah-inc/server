package bus

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/bus/subjects"
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

type ListenStopChan chan interface{}

func (b Bus) Subscribe(subject subjects.Subject, stop ListenStopChan, cb func(pubsub.Message)) error {
	sub := b.client.Subscription(string(subject))
	cctx, cancel := context.WithCancel(b.context)
	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		cb(*msg)
	})
	if err != nil {
		return err
	}

	go func() {
		<-stop

		cancel()
	}()

	return nil
}
