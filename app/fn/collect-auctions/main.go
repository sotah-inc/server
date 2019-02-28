package collectauctions

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
)

var projectId = os.Getenv("GCP_PROJECT")

var regions sotah.RegionList
var busClient bus.Client
var blizzardClient blizzard.Client
var storeClient store.Client
var auctionsStoreBase store.AuctionsBase
var liveAuctionsComputeTopic *pubsub.Topic

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-live-auctions-compute-intake")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	liveAuctionsComputeTopic, err = busClient.FirmTopic(string(subjects.LiveAuctionsComputeIntake))
	if err != nil {
		log.Fatalf("Failed to get firm live-auctions-compute topic: %s", err.Error())

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

func CollectAuctions(_ context.Context, m PubSubMessage) error {
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

		return sotah.Region{}, errors.New("could not resolve region from job")
	}()
	if err != nil {
		return err
	}

	realm := sotah.Realm{
		Realm:  blizzard.Realm{Slug: blizzard.RealmSlug(job.RealmSlug)},
		Region: region,
	}

	bkt, err := auctionsStoreBase.ResolveBucket(realm)
	if err != nil {
		return err
	}

	uri, err := blizzardClient.AppendAccessToken(blizzard.DefaultGetAuctionInfoURL(region.Hostname, blizzard.RealmSlug(job.RealmSlug)))
	if err != nil {
		return err
	}

	aucInfo, respMeta, err := blizzard.NewAuctionInfoFromHTTP(uri)
	if err != nil {
		return err
	}
	if respMeta.Status != http.StatusOK {
		return errors.New("response status for auc-info was not OK")
	}

	if len(aucInfo.Files) == 0 {
		return errors.New("auc-info files was blank")
	}
	aucInfoFile := aucInfo.Files[0]

	obj := auctionsStoreBase.GetObject(aucInfoFile.LastModifiedAsTime(), bkt)
	exists, err := auctionsStoreBase.ObjectExists(obj)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	aucs, respMeta, err := aucInfoFile.GetAuctions()
	if err != nil {
		return err
	}
	if respMeta.Status != http.StatusOK {
		return errors.New("response status for aucs was not OK")
	}

	if err := auctionsStoreBase.Handle(aucs, aucInfoFile.LastModifiedAsTime(), bkt); err != nil {
		return err
	}

	logging.WithFields(logrus.Fields{
		"region":        region.Name,
		"realm":         realm.Slug,
		"last-modified": aucInfoFile.LastModifiedAsTime().Unix(),
	}).Info("Handled, pushing into live-auctions-compute")

	liveAuctionsComputeJob := bus.LoadRegionRealmTimestampsInJob{
		RegionName:      string(region.Name),
		RealmSlug:       string(realm.Slug),
		TargetTimestamp: int(aucInfoFile.LastModifiedAsTime().Unix()),
	}
	jsonEncoded, err := json.Marshal(liveAuctionsComputeJob)
	if err != nil {
		return err
	}

	msg := bus.NewMessage()
	msg.Data = string(jsonEncoded)
	if _, err := busClient.Publish(liveAuctionsComputeTopic, msg); err != nil {
		return err
	}

	return nil
}
