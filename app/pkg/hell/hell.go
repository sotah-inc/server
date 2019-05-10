package hell

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/firestore"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/hell/collections"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/util"
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

func (c Client) GetRealm(realmRef *firestore.DocumentRef) (Realm, error) {
	docsnap, err := realmRef.Get(c.Context)
	if err != nil {
		return Realm{}, err
	}

	var realmData Realm
	if err := docsnap.DataTo(&realmData); err != nil {
		return Realm{}, err
	}

	return realmData, nil
}

type WriteRealmsJob struct {
	Err     error
	Payload WriteRealmsPayload
}

func NewWriteRealmsPayloads(realms sotah.Realms) WriteRealmsPayloads {
	out := WriteRealmsPayloads{}
	for _, realm := range realms {
		out = append(out, NewWriteRealmsPayload(realm))
	}

	return out
}

type WriteRealmsPayloads []WriteRealmsPayload

func NewWriteRealmsPayload(realm sotah.Realm) WriteRealmsPayload {
	return WriteRealmsPayload{
		RegionName:             realm.Region.Name,
		RealmSlug:              realm.Slug,
		RealmModificationDates: realm.RealmModificationDates,
	}
}

type WriteRealmsPayload struct {
	RegionName             blizzard.RegionName
	RealmSlug              blizzard.RealmSlug
	RealmModificationDates sotah.RealmModificationDates
}

func (c Client) WriteRealms(payloads []WriteRealmsPayload) error {
	// spawning workers
	in := make(chan WriteRealmsPayload)
	out := make(chan WriteRealmsJob)
	worker := func() {
		for payload := range in {
			realmRef, err := c.FirmDocument(fmt.Sprintf(
				"%s/%s/%s/%s/%s/%s",
				collections.Games,
				gameversions.Retail,
				collections.Regions,
				payload.RegionName,
				collections.Realms,
				payload.RealmSlug,
			))
			if err != nil {
				out <- WriteRealmsJob{
					Err:     err,
					Payload: payload,
				}

				continue
			}

			if _, err := realmRef.Set(c.Context, NewRealm(payload)); err != nil {
				out <- WriteRealmsJob{
					Err:     err,
					Payload: payload,
				}

				continue
			}

			out <- WriteRealmsJob{
				Err:     nil,
				Payload: payload,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// spinning it up
	go func() {
		for _, payload := range payloads {
			in <- payload
		}

		close(in)
	}()

	// waiting for results to drain out
	for job := range out {
		if job.Err != nil {
			return job.Err
		}
	}

	return nil
}

type Region struct {
	Name string `firestore:"name"`
}

func NewRegion(region sotah.Region) Region {
	return Region{Name: string(region.Name)}
}

func NewRealm(payload WriteRealmsPayload) Realm {
	return Realm{
		Slug:                       string(payload.RealmSlug),
		Downloaded:                 int(payload.RealmModificationDates.Downloaded),
		LiveAuctionsReceived:       int(payload.RealmModificationDates.LiveAuctionsReceived),
		PricelistHistoriesReceived: int(payload.RealmModificationDates.PricelistHistoriesReceived),
	}
}

type Realm struct {
	Slug                       string `firestore:"slug"`
	Downloaded                 int    `firestore:"downloaded"`
	LiveAuctionsReceived       int    `firestore:"live_auctions_received"`
	PricelistHistoriesReceived int    `firestore:"pricelist_histories_received"`
}
