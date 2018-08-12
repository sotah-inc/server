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

	return store{projectID, client}, nil
}

type store struct {
	projectID string
	client    *storage.Client
}
