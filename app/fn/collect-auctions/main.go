package collectauctions

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"github.com/sotah-inc/server/app/pkg/sotah"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/state/subjects"

	"github.com/sotah-inc/server/app/pkg/state"

	"github.com/sotah-inc/server/app/pkg/store"

	"github.com/sotah-inc/server/app/pkg/bus"
)

var projectId = os.Getenv("GCP_PROJECT")

var regions sotah.RegionList
var busClient bus.Client
var blizzardClient blizzard.Client
var storeClient store.Client
var auctionsStoreBase store.AuctionsBase

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-live-auctions-compute-intake")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}

	msg, err := busClient.RequestFromTopic(string(subjects.Boot), "", 5*time.Second)
	if err != nil {
		log.Fatalf("Failed to request boot data: %s", err.Error())

		return
	}

	var bootResponse state.AuthenticatedBootResponse
	if err := json.Unmarshal([]byte(msg.Data), &bootResponse); err != nil {
		log.Fatalf("Failed to unmarshal boot data: %s", err.Error())

		return
	}

	regions = bootResponse.Regions

	blizzardClient, err = blizzard.NewClient(bootResponse.BlizzardClientId, bootResponse.BlizzardClientSecret)
	if err != nil {
		log.Fatalf("Failed to create blizzard client: %s", err.Error())

		return
	}

	auctionsStoreBase = store.NewAuctionsBase(storeClient)
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func AuctionsCollector(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	var job bus.CollectAuctionsJob
	if err := json.Unmarshal([]byte(in.Data), &job); err != nil {
		return err
	}

	region, err := func() (sotah.Region, error) {
		for _, reg := range regions {
			if reg.Name == blizzard.RegionName(job.RegionName) {
				return reg, nil
			}
		}

		return sotah.Region{}, errors.New("region not found")
	}()
	if err != nil {
		return err
	}

	uri := blizzard.DefaultGetAuctionInfoURL(region.Hostname, blizzard.RealmSlug(job.RealmSlug))

	return nil
}
