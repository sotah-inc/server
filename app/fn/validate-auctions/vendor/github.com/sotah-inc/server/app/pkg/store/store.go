package store

import (
	"context"

	"cloud.google.com/go/storage"
)

func NewClient(projectID string) (Client, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return Client{}, err
	}

	s := Client{Context: ctx, projectID: projectID, client: client}

	itemsBucket, err := s.resolveItemsBucket()
	if err != nil {
		return Client{}, err
	}
	s.itemsBucket = itemsBucket

	itemIconsBucket, err := s.resolveItemIconsBucket()
	if err != nil {
		return Client{}, err
	}
	s.itemIconsBucket = itemIconsBucket

	return s, nil
}

type Client struct {
	Context   context.Context
	projectID string
	client    *storage.Client

	itemsBucket        *storage.BucketHandle
	itemIconsBucket    *storage.BucketHandle
	testAuctionsBucket *storage.BucketHandle
}
