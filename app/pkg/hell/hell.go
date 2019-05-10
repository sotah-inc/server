package hell

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/firestore"
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
	Err   error
	Realm sotah.Realm
}

func (c Client) WriteRealms(realms sotah.Realms) error {
	// spawning workers
	in := make(chan sotah.Realm)
	out := make(chan WriteRealmsJob)
	worker := func() {
		for realm := range in {
			realmRef, err := c.FirmDocument(fmt.Sprintf(
				"%s/%s/%s/%s/%s/%s",
				collections.Games,
				gameversions.Retail,
				collections.Regions,
				realm.Region.Name,
				collections.Realms,
				realm.Slug,
			))
			if err != nil {
				out <- WriteRealmsJob{
					Err:   err,
					Realm: realm,
				}

				continue
			}

			if _, err := realmRef.Set(c.Context, NewRealm(realm)); err != nil {
				out <- WriteRealmsJob{
					Err:   err,
					Realm: realm,
				}

				continue
			}

			out <- WriteRealmsJob{
				Err:   nil,
				Realm: realm,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// spinning it up
	go func() {
		for _, realm := range realms {
			in <- realm
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

type Realm struct {
	Slug                       string `firestore:"slug"`
	Downloaded                 int    `firestore:"downloaded"`
	LiveAuctionsReceived       int    `firestore:"live_auctions_received"`
	PricelistHistoriesReceived int    `firestore:"pricelist_histories_received"`
}

func NewRealm(realm sotah.Realm) Realm {
	return Realm{
		Slug:                       string(realm.Slug),
		Downloaded:                 int(realm.RealmModificationDates.Downloaded),
		LiveAuctionsReceived:       int(realm.RealmModificationDates.LiveAuctionsReceived),
		PricelistHistoriesReceived: int(realm.RealmModificationDates.PricelistHistoriesReceived),
	}
}
