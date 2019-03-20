package cleanup_all_expired_manifests

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient            bus.Client
	auctionsCleanupTopic *pubsub.Topic

	auctionManifestStoreBase store.AuctionManifestBaseV2
	auctionManifestBucket    *storage.BucketHandle

	regionRealms map[blizzard.RegionName]sotah.Realms
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-cleanup-all-expired-manifests")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	auctionsCleanupTopic, err = busClient.FirmTopic(string(subjects.CleanupExpiredManifest))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}

	storeClient, err := store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}

	bootBase := store.NewBootBase(storeClient, "us-central1")
	var bootBucket *storage.BucketHandle
	bootBucket, err = bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}
	regionRealms, err = bootBase.GetRegionRealms(bootBucket)
	if err != nil {
		log.Fatalf("Failed to get region-realms: %s", err.Error())

		return
	}

	auctionManifestStoreBase = store.NewAuctionManifestBaseV2(storeClient, "us-central1")
	auctionManifestBucket, err = auctionManifestStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm auction-manifest bucket: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func CleanupAllExpiredManifests(_ context.Context, _ PubSubMessage) error {
	regionExpiredTimestamps, err := auctionManifestStoreBase.GetAllExpiredTimestamps(regionRealms, auctionManifestBucket)
	if err != nil {
		return err
	}

	jobs := []bus.CleanupAuctionManifestJob{}
	for regionName, realmExpiredTimestamps := range regionExpiredTimestamps {
		for realmSlug, expiredTimestamps := range realmExpiredTimestamps {
			for _, timestamp := range expiredTimestamps {
				job := bus.CleanupAuctionManifestJob{
					RegionName:      string(regionName),
					RealmSlug:       string(realmSlug),
					TargetTimestamp: int(timestamp),
				}
				jobs = append(jobs, job)
			}
		}
	}

	for outJob := range busClient.LoadAuctionsCleanupJobs(jobs, auctionsCleanupTopic) {
		if outJob.Err != nil {
			return outJob.Err
		}
	}

	return nil
}
