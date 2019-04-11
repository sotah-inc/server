package boot_intake

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
)

var (
	projectId            = os.Getenv("GCP_PROJECT")
	blizzardClientId     = os.Getenv("BLIZZARD_CLIENT_ID")
	blizzardClientSecret = os.Getenv("BLIZZARD_CLIENT_SECRET")

	busClient bus.Client

	storeClient  store.Client
	bootBase     store.BootBase
	bootBucket   *storage.BucketHandle
	realmsBase   store.RealmsBase
	realmsBucket *storage.BucketHandle
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-boot-intake")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}

	bootBase = store.NewBootBase(storeClient, "us-central1")
	bootBucket, err = bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}

	realmsBase = store.NewRealmsBase(storeClient, "us-central1", gameversions.Retail)
	realmsBucket, err = realmsBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}
}

func GetRegions() (sotah.RegionList, error) {
	msg, err := busClient.RequestFromTopic(string(subjects.Boot), "", 5*time.Second)
	if err != nil {
		return sotah.RegionList{}, err
	}

	var out state.BootResponse
	if err := json.Unmarshal([]byte(msg.Data), &out); err != nil {
		return sotah.RegionList{}, err
	}

	return out.Regions, nil
}

func GetRegionRealms(regions sotah.RegionList) (map[blizzard.RegionName]sotah.Realms, error) {
	regionRealms := map[blizzard.RegionName]sotah.Realms{}
	for job := range busClient.LoadStatuses(regions) {
		if job.Err != nil {
			return map[blizzard.RegionName]sotah.Realms{}, job.Err
		}

		realms := sotah.Realms{}
		for _, realm := range job.Status.Realms {
			realms = append(realms, realm)
		}

		regionRealms[job.Region.Name] = realms
	}

	return regionRealms, nil
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func BootIntake(_ context.Context, _ PubSubMessage) error {
	logging.Info("Fetching regions and writing")
	regions, err := GetRegions()
	if err != nil {
		return err
	}

	gzipEncoded, err := regions.EncodeForStorage()
	if err != nil {
		return err
	}

	wc := bootBase.GetObject("regions.json.gz", bootBucket).NewWriter(storeClient.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	if _, err := wc.Write(gzipEncoded); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	logging.WithField("regions", len(regions)).Info("Fetching region-realms and writing")
	regionRealms, err := GetRegionRealms(regions)
	if err != nil {
		return err
	}

	if err := realmsBase.WriteRealmsMap(regionRealms, realmsBucket); err != nil {
		return err
	}

	logging.Info("Writing blizzard-credentials")
	jsonEncodedCredentials, err := json.Marshal(sotah.BlizzardCredentials{
		ClientId:     blizzardClientId,
		ClientSecret: blizzardClientSecret,
	})
	if err != nil {
		return err
	}

	wc = bootBase.GetObject("blizzard-credentials.json", bootBucket).NewWriter(storeClient.Context)
	wc.ContentType = "application/json"
	if _, err := wc.Write(jsonEncodedCredentials); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	logging.Info("Finished")

	return nil
}
