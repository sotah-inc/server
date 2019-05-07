package hell

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
	"github.com/sotah-inc/server/app/pkg/sotah"
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

func (c Client) Close() error {
	return c.client.Close()
}

func (c Client) Collection(path string) *firestore.CollectionRef {
	return c.client.Collection(path)
}

func (c Client) FirmCollection(path string) (*firestore.CollectionRef, error) {
	out := c.Collection(path)
	if out == nil {
		return nil, errors.New("collection not found")
	}

	return out, nil
}

func (c Client) Doc(path string) *firestore.DocumentRef {
	return c.client.Doc(path)
}

func (c Client) FirmDocument(path string) (*firestore.DocumentRef, error) {
	out := c.Doc(path)
	if out == nil {
		return nil, errors.New("document not found")
	}

	return out, nil
}

type Region struct {
	Name string `firestore:"name"`
}

func NewRegion(region sotah.Region) Region {
	return Region{Name: string(region.Name)}
}

type Realm struct {
	Slug                       string `firestore:"slug"`
	Downloaded                 int    `firestore:"downloaded"`
	LiveAuctionsReceived       int    `firestore:"live_auctions_received"`
	PricelistHistoriesReceived int    `firestore:"pricelist_histories_received"`
}
