package fn

import (
	"log"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/hell"
	"github.com/sotah-inc/server/app/pkg/hell/collections"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
)

type LoadHellStateConfig struct {
	ProjectId string
}

func NewLoadHellState(config LoadHellStateConfig) (LoadHellState, error) {
	// establishing an initial state
	sta := LoadHellState{
		State: state.NewState(uuid.NewV4(), true),
	}

	var err error

	sta.IO.StoreClient, err = store.NewClient(config.ProjectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return LoadHellState{}, err
	}

	sta.IO.HellClient, err = hell.NewClient(config.ProjectId)
	if err != nil {
		log.Fatalf("Failed to create new hell client: %s", err.Error())

		return LoadHellState{}, err
	}

	sta.bootBase = store.NewBootBase(sta.IO.StoreClient, "us-central1")
	sta.bootBucket, err = sta.bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return LoadHellState{}, err
	}

	sta.realmsBase = store.NewRealmsBase(sta.IO.StoreClient, "us-central1", gameversions.Retail)
	sta.realmsBucket, err = sta.realmsBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return LoadHellState{}, err
	}

	return sta, nil
}

type LoadHellState struct {
	state.State

	bootBase   store.BootBase
	bootBucket *storage.BucketHandle

	realmsBase   store.RealmsBase
	realmsBucket *storage.BucketHandle
}

func (sta LoadHellState) Run() error {
	gamesRef := sta.IO.HellClient.Collection(string(collections.Games))
	retailGameRef := gamesRef.Doc(string(gameversions.Retail))
	regionsRef := retailGameRef.Collection(string(collections.Regions))

	regions, err := sta.bootBase.GetRegions(sta.bootBucket)
	if err != nil {
		return err
	}

	for _, region := range regions {
		regionRef := regionsRef.Doc(string(region.Name))

		realms, err := sta.realmsBase.GetAllRealms(region.Name, sta.realmsBucket)
		if err != nil {
			return err
		}

		realmsRef := regionRef.Collection(string(collections.Realms))
		for _, realm := range realms {
			realmRef := realmsRef.Doc(string(realm.Slug))
			if _, err := realmRef.Set(sta.IO.HellClient.Context, hell.NewRealm(realm)); err != nil {
				return err
			}
		}
	}

	return nil
}
