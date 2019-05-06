package hell

import (
	"context"

	"cloud.google.com/go/firestore"
)

func NewClient(projectId string) (Client, error) {
	ctx := context.Background()
	firestoreClient, err := firestore.NewClient(ctx, projectId)
	if err != nil {
		return Client{}, err
	}

	return Client{
		Context:   ctx,
		client:    firestoreClient,
		projectID: projectId,
	}, nil
}

type Client struct {
	Context   context.Context
	projectID string
	client    *firestore.Client
}
