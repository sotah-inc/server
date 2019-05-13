package fn

import (
	"log"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/store/regions"
	"github.com/twinj/uuid"
)

type DownloadAuctionsStateConfig struct {
	ProjectId string
}

func NewDownloadAuctionsState(config DownloadAuctionsStateConfig) (DownloadAuctionsState, error) {
	// establishing an initial state
	sta := DownloadAuctionsState{
		State: state.NewState(uuid.NewV4(), true),
	}

	var err error

	sta.IO.BusClient, err = bus.NewClient(config.ProjectId, "fn-download-auctions")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return DownloadAuctionsState{}, err
	}

	sta.IO.StoreClient, err = store.NewClient(config.ProjectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return DownloadAuctionsState{}, err
	}

	sta.bootBase = store.NewBootBase(sta.IO.StoreClient, "us-central1")
	sta.bootBucket, err = sta.bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return DownloadAuctionsState{}, err
	}

	sta.realmsBase = store.NewRealmsBase(sta.IO.StoreClient, "us-central1", gameversions.Retail)
	sta.realmsBucket, err = sta.realmsBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return DownloadAuctionsState{}, err
	}

	sta.auctionsStoreBase = store.NewAuctionsBaseV2(sta.IO.StoreClient, regions.USCentral1, gameversions.Retail)
	sta.auctionsBucket, err = sta.auctionsStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return DownloadAuctionsState{}, err
	}

	sta.auctionManifestStoreBase = store.NewAuctionManifestBaseV2(
		sta.IO.StoreClient,
		regions.USCentral1,
		gameversions.Retail,
	)
	sta.auctionsManifestBucket, err = sta.auctionManifestStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return DownloadAuctionsState{}, err
	}

	sta.regions, err = sta.bootBase.GetRegions(sta.bootBucket)
	if err != nil {
		log.Fatalf("Failed to get regions: %s", err.Error())

		return DownloadAuctionsState{}, err
	}

	blizzardCredentials, err := sta.bootBase.GetBlizzardCredentials(sta.bootBucket)
	if err != nil {
		log.Fatalf("Failed to get blizzard-credentials: %s", err.Error())

		return DownloadAuctionsState{}, err
	}

	sta.blizzardClient, err = blizzard.NewClient(blizzardCredentials.ClientId, blizzardCredentials.ClientSecret)
	if err != nil {
		log.Fatalf("Failed to create blizzard client: %s", err.Error())

		return DownloadAuctionsState{}, err
	}

	return sta, nil
}

type DownloadAuctionsState struct {
	state.State

	bootBase   store.BootBase
	bootBucket *storage.BucketHandle

	realmsBase   store.RealmsBase
	realmsBucket *storage.BucketHandle

	auctionsStoreBase store.AuctionsBaseV2
	auctionsBucket    *storage.BucketHandle

	auctionManifestStoreBase store.AuctionManifestBaseV2
	auctionsManifestBucket   *storage.BucketHandle

	regions sotah.RegionList

	blizzardClient blizzard.Client
}
