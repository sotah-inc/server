package collectauctions

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
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
var auctionsStoreBase store.AuctionsBaseV2
var auctionsBucket *storage.BucketHandle
var auctionManifestStoreBase store.AuctionManifestBaseV2
var auctionsManifestBucket *storage.BucketHandle

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-download-auctions")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}
	auctionsStoreBase = store.NewAuctionsBaseV2(storeClient)
	auctionManifestStoreBase = store.NewAuctionManifestBaseV2(storeClient)

	auctionsBucket, err = auctionsStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm raw-auctions bucket: %s", err.Error())

		return
	}

	auctionsManifestBucket, err = auctionManifestStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm auctions-manifest bucket: %s", err.Error())

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

	regions = bootResponse.Regions

	blizzardClient, err = blizzard.NewClient(bootResponse.BlizzardClientId, bootResponse.BlizzardClientSecret)
	if err != nil {
		log.Fatalf("Failed to create blizzard client: %s", err.Error())

		return
	}
}

func Handle(job bus.CollectAuctionsJob) bus.Message {
	m := bus.NewMessage()

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
		m.Err = err.Error()
		m.Code = codes.NotFound

		return m
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
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	obj := auctionsStoreBase.GetObject(realm, aucInfoFile.LastModifiedAsTime(), auctionsBucket)
	exists, err := auctionsStoreBase.ObjectExists(obj)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}
	if exists {
		logging.WithFields(logrus.Fields{
			"region":        region.Name,
			"realm":         realm.Slug,
			"last-modified": aucInfoFile.LastModifiedAsTime().Unix(),
		}).Info("Object exists for region/ realm/ last-modified tuple, skipping")

		m.Code = codes.Ok

		return m
	}

	resp, err := blizzard.Download(aucInfoFile.URL)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}
	if resp.Status != http.StatusOK {
		m.Err = errors.New("response status for aucs was not OK").Error()
		m.Code = codes.GenericError

		return m
	}
	aucs, err := blizzard.NewAuctions(resp.Body)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	if err := auctionsStoreBase.Handle(aucs, aucInfoFile.LastModifiedAsTime(), realm, auctionsBucket); err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	logging.WithFields(logrus.Fields{
		"region":        region.Name,
		"realm":         realm.Slug,
		"last-modified": aucInfoFile.LastModifiedAsTime().Unix(),
	}).Info("Handled, adding to auction-manifest file")

	if err := auctionManifestStoreBase.Handle(
		sotah.UnixTimestamp(aucInfoFile.LastModifiedAsTime().Unix()),
		realm,
		auctionsManifestBucket,
	); err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	return m
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func DownloadAuctions(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	var job bus.CollectAuctionsJob
	if err := json.Unmarshal([]byte(in.Data), &job); err != nil {
		return err
	}

	msg := Handle(job)
	msg.ReplyToId = in.ReplyToId
	if _, err := busClient.ReplyTo(in, msg); err != nil {
		return err
	}

	return nil
}
