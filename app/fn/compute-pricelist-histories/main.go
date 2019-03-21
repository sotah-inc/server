package compute_pricelist_histories

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/store"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient bus.Client

	storeClient                 store.Client
	auctionsStoreBase           store.AuctionsBaseV2
	rawAuctionsBucket           *storage.BucketHandle
	pricelistHistoriesStoreBase store.PricelistHistoriesBaseV2
	pricelistHistoriesBucket    *storage.BucketHandle
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-compute-pricelist-histories")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}

	pricelistHistoriesStoreBase = store.NewPricelistHistoriesBaseV2(storeClient, "us-central1")
	pricelistHistoriesBucket, err = pricelistHistoriesStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm pricelist-histories bucket: %s", err.Error())

		return
	}

	auctionsStoreBase = store.NewAuctionsBaseV2(storeClient, "us-central1")
	rawAuctionsBucket, err = auctionsStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get auctions bucket: %s", err.Error())

		return
	}
}

func Handle(job bus.LoadRegionRealmTimestampsInJob) bus.Message {
	m := bus.NewMessage()

	region := sotah.Region{Name: blizzard.RegionName(job.RegionName)}
	realm := sotah.Realm{
		Realm:  blizzard.Realm{Slug: blizzard.RealmSlug(job.RealmSlug)},
		Region: region,
	}
	targetTime := time.Unix(int64(job.TargetTimestamp), 0)

	obj, err := auctionsStoreBase.GetFirmObject(realm, targetTime, rawAuctionsBucket)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.NotFound

		return m
	}

	aucs, err := storeClient.NewAuctions(obj)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	normalizedTargetTimestamp, err := pricelistHistoriesStoreBase.Handle(aucs, targetTime, realm, pricelistHistoriesBucket)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	req := database.PricelistHistoriesComputeIntakeRequest{
		RegionName:                string(region.Name),
		RealmSlug:                 string(realm.Slug),
		NormalizedTargetTimestamp: int(normalizedTargetTimestamp),
	}
	jsonEncodedRequest, err := json.Marshal(req)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}
	m.Data = string(jsonEncodedRequest)

	return m
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func ComputePricelistHistories(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	job, err := bus.NewLoadRegionRealmTimestampsInJob(in.Data)
	if err != nil {
		return err
	}

	logging.WithField("job", job).Info("Handling")

	msg := Handle(job)
	msg.ReplyToId = in.ReplyToId
	if _, err := busClient.ReplyTo(in, msg); err != nil {
		return err
	}

	return nil
}
