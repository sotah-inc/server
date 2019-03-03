package auctionscleanup

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var projectId = os.Getenv("GCP_PROJECT")

var regionRealms map[blizzard.RegionName]sotah.Realms

var busClient bus.Client
var auctionsCleanupTopic *pubsub.Topic

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-auctions-collector")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	auctionsCleanupTopic, err = busClient.FirmTopic(string(subjects.AuctionsCleanupCompute))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}

	bootResponse, err := func() (state.AuthenticatedBootResponse, error) {
		msg, err := busClient.RequestFromTopic(string(subjects.Boot), "", 5*time.Second)
		if err != nil {
			return state.AuthenticatedBootResponse{}, err
		}

		var out state.AuthenticatedBootResponse
		if err := json.Unmarshal([]byte(msg.Data), &out); err != nil {
			return state.AuthenticatedBootResponse{}, err
		}

		return out, nil
	}()
	if err != nil {
		log.Fatalf("Failed to get authenticated-boot-response: %s", err.Error())

		return
	}

	regions := bootResponse.Regions

	regionRealms = map[blizzard.RegionName]sotah.Realms{}
	for job := range busClient.LoadStatuses(regions) {
		if job.Err != nil {
			log.Fatalf("Failed to fetch status: %s", job.Err.Error())

			return
		}

		regionRealms[job.Region.Name] = job.Status.Realms
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func AuctionsCleanup(_ context.Context, _ PubSubMessage) error {
	priorManifestTimestamps := []sotah.UnixTimestamp{}
	normalizedTime := sotah.NormalizeTargetDate(time.Now())
	startOffset := 14
	endLimit := 14
	for i := 0; i < endLimit; i++ {
		then := normalizedTime.AddDate(0, 0, -1*(i+startOffset))

		priorManifestTimestamps = append(priorManifestTimestamps, sotah.UnixTimestamp(then.Unix()))
	}

	jobs := []bus.CleanupAuctionManifestJob{}

	for _, realms := range regionRealms {
		for _, realm := range realms {
			for _, expiredManifestTimestamp := range priorManifestTimestamps {
				job := bus.CleanupAuctionManifestJob{
					RegionName:      string(realm.Region.Name),
					RealmSlug:       string(realm.Slug),
					TargetTimestamp: int(expiredManifestTimestamp),
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
