package fn

import (
	"log"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/store/regions"
	"github.com/twinj/uuid"
)

type ComputeLiveAuctionsStateConfig struct {
	ProjectId string
}

func NewComputeLiveAuctionsState(config ComputeLiveAuctionsStateConfig) (ComputeLiveAuctionsState, error) {
	// establishing an initial state
	sta := ComputeLiveAuctionsState{
		State: state.NewState(uuid.NewV4(), true),
	}

	var err error

	sta.IO.BusClient, err = bus.NewClient(config.ProjectId, "fn-compute-live-auctions")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return ComputeLiveAuctionsState{}, err
	}

	sta.IO.StoreClient, err = store.NewClient(config.ProjectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return ComputeLiveAuctionsState{}, err
	}

	sta.auctionsStoreBase = store.NewAuctionsBaseV2(sta.IO.StoreClient, regions.USCentral1, gameversions.Retail)
	sta.auctionsBucket, err = sta.auctionsStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return ComputeLiveAuctionsState{}, err
	}

	sta.liveAuctionsStoreBase = store.NewLiveAuctionsBase(sta.IO.StoreClient, regions.USCentral1, gameversions.Retail)
	sta.liveAuctionsBucket, err = sta.liveAuctionsStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return ComputeLiveAuctionsState{}, err
	}

	return sta, nil
}

type ComputeLiveAuctionsState struct {
	state.State

	auctionsStoreBase store.AuctionsBaseV2
	auctionsBucket    *storage.BucketHandle

	liveAuctionsStoreBase store.LiveAuctionsBase
	liveAuctionsBucket    *storage.BucketHandle
}
