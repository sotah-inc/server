package main

import (
	"context"

	storage "cloud.google.com/go/storage"
)

func newStore(projectID string) (store, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return store{}, err
	}

	s := store{context: ctx, projectID: projectID, client: client}

	itemsBucket, err := s.resolveItemsBucket()
	if err != nil {
		return store{}, err
	}
	s.itemsBucket = itemsBucket

	itemIconsBucket, err := s.resolveItemIconsBucket()
	if err != nil {
		return store{}, err
	}
	s.itemIconsBucket = itemIconsBucket

	return s, nil
}

type store struct {
	context   context.Context
	projectID string
	client    *storage.Client

	itemsBucket     *storage.BucketHandle
	itemIconsBucket *storage.BucketHandle
}
