package store

import (
	"context"

	"cloud.google.com/go/storage"
)

func NewStore(projectID string) (Store, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return Store{}, err
	}

	s := Store{Context: ctx, projectID: projectID, client: client}

	itemsBucket, err := s.resolveItemsBucket()
	if err != nil {
		return Store{}, err
	}
	s.itemsBucket = itemsBucket

	itemIconsBucket, err := s.resolveItemIconsBucket()
	if err != nil {
		return Store{}, err
	}
	s.itemIconsBucket = itemIconsBucket

	return s, nil
}

type Store struct {
	Context   context.Context
	projectID string
	client    *storage.Client

	itemsBucket     *storage.BucketHandle
	itemIconsBucket *storage.BucketHandle
}
