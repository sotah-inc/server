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
