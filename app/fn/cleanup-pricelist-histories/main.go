package cleanup_pricelist_histories

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"

	"github.com/sotah-inc/server/app/pkg/store/regions"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/store"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient bus.Client

	storeClient store.Client

	pricelistHistoriesBase   store.PricelistHistoriesBaseV2
	pricelistHistoriesBucket *storage.BucketHandle
)

func init() {
	var err error

	busClient, err = bus.NewClient(projectId, "fn-cleanup-pricelist-histories")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}

	pricelistHistoriesBase = store.NewPricelistHistoriesBaseV2(storeClient, regions.USCentral1, gameversions.Retail)
	pricelistHistoriesBucket, err = pricelistHistoriesBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm raw-auctions bucket: %s", err.Error())

		return
	}
}

func Handle(job sotah.CleanupPricelistPayload) bus.Message {
	m := bus.NewMessage()

	logging.WithFields(logrus.Fields{"job": job}).Info("Handling")

	realm := sotah.NewSkeletonRealm(blizzard.RegionName(job.RegionName), blizzard.RealmSlug(job.RealmSlug))
	expiredTimestamps, err := pricelistHistoriesBase.GetExpiredTimestamps(realm, pricelistHistoriesBucket)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	totalDeleted, err := pricelistHistoriesBase.DeleteAll(realm, expiredTimestamps, pricelistHistoriesBucket)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	res := sotah.CleanupPricelistPayloadResponse{
		RegionName:   job.RegionName,
		RealmSlug:    job.RealmSlug,
		TotalDeleted: totalDeleted,
	}
	data, err := res.EncodeForDelivery()
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}
	m.Data = data

	return m
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func CleanupPricelistHistories(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	job, err := sotah.NewCleanupPricelistPayload(in.Data)
	if err != nil {
		return err
	}

	reply := Handle(job)
	reply.ReplyToId = in.ReplyToId
	if _, err := busClient.ReplyTo(in, reply); err != nil {
		return err
	}

	return nil
}
