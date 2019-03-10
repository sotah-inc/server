package compute_live_auctions

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
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/store"
)

var projectId = os.Getenv("GCP_PROJECT")

var busClient bus.Client

var storeClient store.Client
var liveAuctionsStoreBase store.LiveAuctionsBase
var auctionsStoreBase store.AuctionsBaseV2
var auctionsBucket *storage.BucketHandle

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-compute-live-auctions")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}
	liveAuctionsStoreBase = store.NewLiveAuctionsBase(storeClient)
	auctionsStoreBase = store.NewAuctionsBaseV2(storeClient)

	auctionsBucket, err = auctionsStoreBase.GetFirmBucket()
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

	obj, err := auctionsStoreBase.GetFirmObject(realm, targetTime, auctionsBucket)
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

	if err := liveAuctionsStoreBase.Handle(aucs, realm); err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	respData := bus.RegionRealmTimestampTuple{
		RegionName:      string(realm.Region.Name),
		RealmSlug:       string(realm.Slug),
		TargetTimestamp: job.TargetTimestamp,
	}
	data, err := json.Marshal(respData)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}
	m.Data = string(data)

	return m
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func ComputeLiveAuctions(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	var job bus.LoadRegionRealmTimestampsInJob
	if err := json.Unmarshal([]byte(in.Data), &job); err != nil {
		return err
	}

	msg := Handle(job)
	msg.ReplyToId = in.ReplyToId
	if _, err := busClient.ReplyTo(in, msg); err != nil {
		return err
	}

	return nil
}
