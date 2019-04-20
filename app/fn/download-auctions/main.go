package collectauctions

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	gcpRegions "github.com/sotah-inc/server/app/pkg/store/regions"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/store"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient bus.Client

	blizzardClient blizzard.Client

	storeClient store.Client

	auctionsStoreBase store.AuctionsBaseV2
	auctionsBucket    *storage.BucketHandle

	auctionManifestStoreBase store.AuctionManifestBaseV2
	auctionsManifestBucket   *storage.BucketHandle

	liveAuctionsStoreBase store.LiveAuctionsBase
	liveAuctionsBucket    *storage.BucketHandle

	pricelistHistoriesStoreBase store.PricelistHistoriesBaseV2
	pricelistHistoriesBucket    *storage.BucketHandle

	regions      sotah.RegionList
	regionRealms sotah.RegionRealms
)

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

	auctionsStoreBase = store.NewAuctionsBaseV2(storeClient, gcpRegions.USCentral1, gameversions.Retail)
	auctionsBucket, err = auctionsStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}

	auctionManifestStoreBase = store.NewAuctionManifestBaseV2(storeClient, gcpRegions.USCentral1, gameversions.Retail)
	auctionsManifestBucket, err = auctionManifestStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}

	liveAuctionsStoreBase = store.NewLiveAuctionsBase(storeClient, gcpRegions.USCentral1, gameversions.Retail)
	liveAuctionsBucket, err = liveAuctionsStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}

	pricelistHistoriesStoreBase = store.NewPricelistHistoriesBaseV2(
		storeClient,
		gcpRegions.USCentral1,
		gameversions.Retail,
	)
	pricelistHistoriesBucket, err = pricelistHistoriesStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}

	bootBase := store.NewBootBase(storeClient, "us-central1")
	var bootBucket *storage.BucketHandle
	bootBucket, err = bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}
	regions, err = bootBase.GetRegions(bootBucket)
	if err != nil {
		log.Fatalf("Failed to get regions: %s", err.Error())

		return
	}
	blizzardCredentials, err := bootBase.GetBlizzardCredentials(bootBucket)
	if err != nil {
		log.Fatalf("Failed to get blizzard-credentials: %s", err.Error())

		return
	}

	realmsBase := store.NewRealmsBase(storeClient, "us-central1", gameversions.Retail)
	realmsBucket, err := realmsBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}

	for _, region := range regions {
		realms, err := realmsBase.GetAllRealms(region.Name, realmsBucket)
		if err != nil {
			log.Fatalf("Failed to get realms: %s", err.Error())

			return
		}

		regionRealms[region.Name] = realms
	}

	logging.Info("Received regions, region-realms, and blizzard-credentials")

	blizzardClient, err = blizzard.NewClient(blizzardCredentials.ClientId, blizzardCredentials.ClientSecret)
	if err != nil {
		log.Fatalf("Failed to create blizzard client: %s", err.Error())

		return
	}
}

func ResolveRegionRealm(job bus.CollectAuctionsJob) (sotah.Region, sotah.Realm, error) {
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

	realm, err := func() (sotah.Realm, error) {
		realms, ok := regionRealms[blizzard.RegionName(job.RegionName)]
		if !ok {
			return sotah.Realm{}, errors.New("could not resolve realms from job")
		}

		for _, realm := range realms {
			if realm.Slug == blizzard.RealmSlug(job.RealmSlug) {
				return realm, nil
			}
		}

		return sotah.Realm{}, errors.New("could not resolve realm from job")
	}()
	if err != nil {
		return sotah.Region{}, sotah.Realm{}, err
	}

	return region, realm, nil
}

func Handle(job bus.CollectAuctionsJob) bus.Message {
	m := bus.NewMessage()

	region, realm, err := ResolveRegionRealm(job)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.NotFound

		return m
	}

	uri, err := blizzardClient.AppendAccessToken(blizzard.DefaultGetAuctionInfoURL(region.Hostname, blizzard.RealmSlug(job.RealmSlug)))
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	aucInfo, respMeta, err := blizzard.NewAuctionInfoFromHTTP(uri)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}
	if respMeta.Status != http.StatusOK {
		m.Err = errors.New("response status for auc-info was not OK").Error()
		m.Code = codes.BlizzardError

		respError := blizzard.ResponseError{
			Status: respMeta.Status,
			Body:   string(respMeta.Body),
			URI:    uri,
		}
		data, err := json.Marshal(respError)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError

			return m
		}

		m.Data = string(data)

		return m
	}

	aucInfoFile, err := func() (blizzard.AuctionFile, error) {
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

	lastModifiedTime := aucInfoFile.LastModifiedAsTime()
	lastModifiedTimestamp := sotah.UnixTimestamp(lastModifiedTime.Unix())

	obj := auctionsStoreBase.GetObject(realm, lastModifiedTime, auctionsBucket)
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
			"last-modified": lastModifiedTimestamp,
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
		m.Code = codes.BlizzardError

		respError := blizzard.ResponseError{
			Status: resp.Status,
			Body:   string(resp.Body),
			URI:    aucInfoFile.URL,
		}
		data, err := json.Marshal(respError)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError

			return m
		}

		m.Data = string(data)

		return m
	}

	logging.WithFields(logrus.Fields{
		"region":        region.Name,
		"realm":         realm.Slug,
		"last-modified": lastModifiedTimestamp,
	}).Info("Received body, parsing")
	aucs, err := blizzard.NewAuctions(resp.Body)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	logging.WithFields(logrus.Fields{
		"region":        region.Name,
		"realm":         realm.Slug,
		"last-modified": lastModifiedTimestamp,
	}).Info("Parsed, saving to raw-auctions store")
	if err := auctionsStoreBase.Handle(resp.Body, lastModifiedTime, realm, auctionsBucket); err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	logging.WithFields(logrus.Fields{
		"region":        region.Name,
		"realm":         realm.Slug,
		"last-modified": lastModifiedTimestamp,
	}).Info("Saved, adding to auction-manifest file")
	if err := auctionManifestStoreBase.Handle(lastModifiedTimestamp, realm, auctionsManifestBucket); err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	logging.WithFields(logrus.Fields{
		"region":        region.Name,
		"realm":         realm.Slug,
		"last-modified": lastModifiedTimestamp,
	}).Info("Saved to manifest, parsing into live-auctions")
	if err := liveAuctionsStoreBase.Handle(aucs, realm, liveAuctionsBucket); err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	logging.WithFields(logrus.Fields{
		"region":        region.Name,
		"realm":         realm.Slug,
		"last-modified": lastModifiedTimestamp,
	}).Info("Parsed into live-auctions, handling pricelist-history")
	normalizedTargetTimestamp, err := pricelistHistoriesStoreBase.Handle(
		aucs,
		lastModifiedTime,
		realm,
		pricelistHistoriesBucket,
	)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	logging.WithFields(logrus.Fields{
		"region":        region.Name,
		"realm":         realm.Slug,
		"last-modified": lastModifiedTimestamp,
	}).Info("Handled pricelist-history")

	respData := bus.RegionRealmTimestampTuple{
		RegionName:                string(realm.Region.Name),
		RealmSlug:                 string(realm.Slug),
		TargetTimestamp:           int(lastModifiedTimestamp),
		ItemIds:                   aucs.ItemIds().ToInts(),
		OwnerNames:                aucs.OwnerNames(),
		NormalizedTargetTimestamp: int(normalizedTargetTimestamp),
	}
	data, err := respData.EncodeForDelivery()
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}
	m.Data = string(data)

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
