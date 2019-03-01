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
var liveAuctionsComputeTopic *pubsub.Topic

var blizzardClient blizzard.Client

var storeClient store.Client
var auctionsStoreBase store.AuctionsBase
var auctionManifestStoreBase store.AuctionManifestBase

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-live-auctions-compute-intake")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	liveAuctionsComputeTopic, err = busClient.FirmTopic(string(subjects.LiveAuctionsCompute))
	if err != nil {
		log.Fatalf("Failed to get firm live-auctions-compute topic: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}
	auctionsStoreBase = store.NewAuctionsBase(storeClient)
	auctionManifestStoreBase = store.NewAuctionManifestBase(storeClient)

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

	regions = bootResponse.Regions

	blizzardClient, err = blizzard.NewClient(bootResponse.BlizzardClientId, bootResponse.BlizzardClientSecret)
	if err != nil {
		log.Fatalf("Failed to create blizzard client: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func CollectAuctions(_ context.Context, m PubSubMessage) error {
	job, err := func() (bus.CollectAuctionsJob, error) {
		var in bus.Message
		if err := json.Unmarshal(m.Data, &in); err != nil {
			return bus.CollectAuctionsJob{}, err
		}

		var out bus.CollectAuctionsJob
		if err := json.Unmarshal([]byte(in.Data), &out); err != nil {
			return bus.CollectAuctionsJob{}, err
		}

		return out, nil
	}()
	if err != nil {
		return err
	}

	region, realm, err := func() (sotah.Region, sotah.Realm, error) {
		region, err := func() (sotah.Region, error) {
			for _, reg := range regions {
				if reg.Name == blizzard.RegionName(job.RegionName) {
					return reg, nil
				}
			}

			return sotah.Region{}, errors.New("could not resolve region from job")
		}()
		if err != nil {
			return sotah.Region{}, sotah.Realm{}, err
		}

		realm := sotah.Realm{
			Realm:  blizzard.Realm{Slug: blizzard.RealmSlug(job.RealmSlug)},
			Region: region,
		}

		return region, realm, nil
	}()
	if err != nil {
		return err
	}

	aucInfoFile, err := func() (blizzard.AuctionFile, error) {
		uri, err := blizzardClient.AppendAccessToken(blizzard.DefaultGetAuctionInfoURL(region.Hostname, blizzard.RealmSlug(job.RealmSlug)))
		if err != nil {
			return blizzard.AuctionFile{}, err
		}

		aucInfo, respMeta, err := blizzard.NewAuctionInfoFromHTTP(uri)
		if err != nil {
			return blizzard.AuctionFile{}, err
		}
		if respMeta.Status != http.StatusOK {
			return blizzard.AuctionFile{}, errors.New("response status for auc-info was not OK")
		}

		if len(aucInfo.Files) == 0 {
			return blizzard.AuctionFile{}, errors.New("auc-info files was blank")
		}

		return aucInfo.Files[0], nil
	}()
	if err != nil {
		return err
	}

	auctionsBucket, err := auctionsStoreBase.ResolveBucket(realm)
	if err != nil {
		return err
	}

	obj := auctionsStoreBase.GetObject(aucInfoFile.LastModifiedAsTime(), auctionsBucket)
	exists, err := auctionsStoreBase.ObjectExists(obj)
	if err != nil {
		return err
	}
	if exists {
		logging.WithFields(logrus.Fields{
			"region":        region.Name,
			"realm":         realm.Slug,
			"last-modified": aucInfoFile.LastModifiedAsTime().Unix(),
		}).Info("Object exists for region/ realm/ last-modified tuple, skipping")

		return nil
	}

	aucs, respMeta, err := aucInfoFile.GetAuctions()
	if err != nil {
		return err
	}
	if respMeta.Status != http.StatusOK {
		return errors.New("response status for aucs was not OK")
	}

	if err := auctionsStoreBase.Handle(aucs, aucInfoFile.LastModifiedAsTime(), auctionsBucket); err != nil {
		return err
	}

	logging.WithFields(logrus.Fields{
		"region":        region.Name,
		"realm":         realm.Slug,
		"last-modified": aucInfoFile.LastModifiedAsTime().Unix(),
	}).Info("Handled, pushing into live-auctions-compute and adding to auction-manifest file")

	msg, err := func() (bus.Message, error) {
		jsonEncoded, err := json.Marshal(bus.LoadRegionRealmTimestampsInJob{
			RegionName:      string(region.Name),
			RealmSlug:       string(realm.Slug),
			TargetTimestamp: int(aucInfoFile.LastModifiedAsTime().Unix()),
		})
		if err != nil {
			return bus.Message{}, err
		}

		msg := bus.NewMessage()
		msg.Data = string(jsonEncoded)

		return msg, nil
	}()
	if err != nil {
		return err
	}

	if _, err := busClient.Publish(liveAuctionsComputeTopic, msg); err != nil {
		return err
	}

	if err := auctionManifestStoreBase.Handle(sotah.UnixTimestamp(aucInfoFile.LastModifiedAsTime().Unix()), realm); err != nil {
		return err
	}

	return nil
}
