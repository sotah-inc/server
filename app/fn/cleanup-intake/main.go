package cleanupintake

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"google.golang.org/api/iterator"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"

	"github.com/sotah-inc/server/app/pkg/bus"

	"github.com/sotah-inc/server/app/pkg/store"
)

var projectId = os.Getenv("GCP_PROJECT")

var storeClient store.Client
var auctionsStoreBase store.AuctionsBase

func init() {
	var err error

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}
	auctionsStoreBase = store.NewAuctionsBase(storeClient)
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func CleanupIntake(_ context.Context, m PubSubMessage) error {
	job, err := func() (bus.CollectAuctionsJob, error) {
		var in bus.Message
		if err := json.Unmarshal(m.Data, &in); err != nil {
			return bus.CollectAuctionsJob{}, err
		}

		var out bus.CollectAuctionsJob
		if err := json.Unmarshal([]byte(in.Data), &out); err != nil {
			return bus.CollectAuctionsJob{}, err
		}

		return out, nil
	}()
	if err != nil {
		return err
	}

	region := sotah.Region{Name: blizzard.RegionName(job.RegionName)}
	realm := sotah.Realm{
		Realm:  blizzard.Realm{Slug: blizzard.RealmSlug(job.RealmSlug)},
		Region: region,
	}

	bkt := auctionsStoreBase.GetBucket(realm)
	it := bkt.Objects(storeClient.Context, nil)
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}

			if err != nil {
				return err
			}
		}

		parts := strings.Split(objAttrs.Name, ".")
		timestamp, err := strconv.Atoi(parts[0])
		if err != nil {
			return err
		}

		logging.WithFields(logrus.Fields{
			"region":    region.Name,
			"realm":     realm.Slug,
			"timestamp": timestamp,
		}).Info("Found")
	}

	return nil
}
