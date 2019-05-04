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

type ComputePricelistHistoriesStateConfig struct {
	ProjectId string
}

func NewComputePricelistHistoriesState(config ComputePricelistHistoriesStateConfig) (ComputePricelistHistoriesState, error) {
	// establishing an initial state
	sta := ComputePricelistHistoriesState{
		State: state.NewState(uuid.NewV4(), true),
	}

	var err error

	sta.IO.BusClient, err = bus.NewClient(config.ProjectId, "fn-compute-pricelist-histories")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return ComputePricelistHistoriesState{}, err
	}

	sta.IO.StoreClient, err = store.NewClient(config.ProjectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return ComputePricelistHistoriesState{}, err
	}

	sta.auctionsStoreBase = store.NewAuctionsBaseV2(sta.IO.StoreClient, regions.USCentral1, gameversions.Retail)
	sta.auctionsBucket, err = sta.auctionsStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return ComputePricelistHistoriesState{}, err
	}

	sta.pricelistHistoriesStoreBase = store.NewPricelistHistoriesBaseV2(
		sta.IO.StoreClient,
		regions.USCentral1,
		gameversions.Retail,
	)
	sta.pricelistHistoriesBucket, err = sta.pricelistHistoriesStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return ComputePricelistHistoriesState{}, err
	}

	return sta, nil
}

type ComputePricelistHistoriesState struct {
	state.State

	auctionsStoreBase store.AuctionsBaseV2
	auctionsBucket    *storage.BucketHandle

	pricelistHistoriesStoreBase store.PricelistHistoriesBaseV2
	pricelistHistoriesBucket    *storage.BucketHandle
}
