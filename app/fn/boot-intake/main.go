package boot_intake

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
)

var (
	projectId            = os.Getenv("GCP_PROJECT")
	blizzardClientId     = os.Getenv("BLIZZARD_CLIENT_ID")
	blizzardClientSecret = os.Getenv("BLIZZARD_CLIENT_SECRET")

	busClient bus.Client

	storeClient store.Client
	bootBase    store.BootBase
	bootBucket  *storage.BucketHandle

	regions      sotah.RegionList
	regionRealms map[blizzard.RegionName]sotah.Realms
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

	bootResponse, err := func() (state.BootResponse, error) {
		msg, err := busClient.RequestFromTopic(string(subjects.Boot), "", 5*time.Second)
		if err != nil {
			return state.BootResponse{}, err
		}

		var out state.BootResponse
		if err := json.Unmarshal([]byte(msg.Data), &out); err != nil {
			return state.BootResponse{}, err
		}

		return out, nil
	}()
	if err != nil {
		log.Fatalf("Failed to get boot response: %s", err.Error())

		return
	}

	regions = bootResponse.Regions

	regionRealms = map[blizzard.RegionName]sotah.Realms{}
	for job := range busClient.LoadStatuses(regions) {
		if job.Err != nil {
			log.Fatalf("Failed to fetch status: %s", job.Err.Error())

			return
		}

		realms := sotah.Realms{}
		for _, realm := range job.Status.Realms {
			realms = append(realms, realm)
		}

		regionRealms[job.Region.Name] = realms
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func BootIntake(_ context.Context, _ PubSubMessage) error {
	jsonEncodedRegions, err := json.Marshal(regions)
	if err != nil {
		return err
	}
	gzipEncodedRegions, err := util.GzipEncode(jsonEncodedRegions)
	if err != nil {
		return err
	}

	wc := bootBase.GetObject("regions.json.gz", bootBucket).NewWriter(storeClient.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	if _, err := wc.Write(gzipEncodedRegions); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	for regionName, realms := range regionRealms {
		jsonEncodedRealms, err := json.Marshal(realms)
		if err != nil {
			return err
		}
		gzipEncodedRealms, err := util.GzipEncode(jsonEncodedRealms)
		if err != nil {
			return err
		}

		wc := bootBase.GetObject(fmt.Sprintf("%s/realms.json.gz", regionName), bootBucket).NewWriter(storeClient.Context)
		wc.ContentType = "application/json"
		wc.ContentEncoding = "gzip"
		if _, err := wc.Write(gzipEncodedRealms); err != nil {
			return err
		}
		if err := wc.Close(); err != nil {
			return err
		}
	}

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

	return nil
}
