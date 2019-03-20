package download_all_auctions

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	blizzardClient blizzard.Client

	busClient                bus.Client
	downloadAuctionsTopic    *pubsub.Topic
	validateAllAuctionsTopic *pubsub.Topic

	storeClient              store.Client
	auctionsStoreBase        store.AuctionsBaseV2
	auctionsBucket           *storage.BucketHandle
	auctionManifestStoreBase store.AuctionManifestBaseV2
	auctionsManifestBucket   *storage.BucketHandle

	regions      sotah.RegionList
	regionRealms map[blizzard.RegionName]sotah.Realms
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-download-all-auctions")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	downloadAuctionsTopic, err = busClient.FirmTopic(string(subjects.DownloadAuctions))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
	validateAllAuctionsTopic, err = busClient.FirmTopic(string(subjects.ValidateAllAuctions))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}

	auctionsStoreBase = store.NewAuctionsBaseV2(storeClient, "us-central1")
	auctionsBucket, err = auctionsStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm raw-auctions bucket: %s", err.Error())

		return
	}

	auctionManifestStoreBase = store.NewAuctionManifestBaseV2(storeClient, "us-central1")
	auctionsManifestBucket, err = auctionManifestStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm auctions-manifest bucket: %s", err.Error())

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
	regionRealms, err = bootBase.GetRegionRealms(bootBucket)
	if err != nil {
		log.Fatalf("Failed to get region-realms: %s", err.Error())

		return
	}
	blizzardCredentials, err := bootBase.GetBlizzardCredentials(bootBucket)
	if err != nil {
		log.Fatalf("Failed to get blizzard-credentials: %s", err.Error())

		return
	}

	blizzardClient, err = blizzard.NewClient(blizzardCredentials.ClientId, blizzardCredentials.ClientSecret)
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
	}()
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

	if err := auctionsStoreBase.Handle(resp.Body, aucInfoFile.LastModifiedAsTime(), realm, auctionsBucket); err != nil {
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

	respData := bus.RegionRealmTimestampTuple{
		RegionName:      string(realm.Region.Name),
		RealmSlug:       string(realm.Slug),
		TargetTimestamp: int(aucInfoFile.LastModifiedAsTime().Unix()),
	}
	data, err := json.Marshal(respData)
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

func DownloadAllAuctions(_ context.Context, _ PubSubMessage) error {
	// producing channels for intake and exit
	in := make(chan sotah.Realm)
	out := make(chan bus.Message)

	// spinning up the workers
	worker := func() {
		for realm := range in {
			job := bus.CollectAuctionsJob{
				RegionName: string(realm.Region.Name),
				RealmSlug:  string(realm.Slug),
			}

			out <- Handle(job)
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// enqueueing realms for processing
	startTime := time.Now()
	go func() {
		for _, realms := range regionRealms {
			for _, realm := range realms {
				in <- realm
			}
		}

		close(in)
	}()

	// enqueueing them and gathering result jobs
	responseItems := bus.BulkRequestMessages{}
	for msg := range out {
		responseItems[msg.ReplyToId] = msg
	}

	// reporting metrics
	m := metric.Metrics{"download_all_auctions_duration": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000)}
	if err := busClient.PublishMetrics(m); err != nil {
		return err
	}

	validatedResponseItems := bus.BulkRequestMessages{}
	for k, msg := range responseItems {
		if msg.Code != codes.Ok {
			if msg.Code == codes.BlizzardError {
				var respError blizzard.ResponseError
				if err := json.Unmarshal([]byte(msg.Data), &respError); err != nil {
					return err
				}

				logging.WithFields(logrus.Fields{"resp-error": respError}).Error("Received erroneous response")
			}

			continue
		}

		// ok msg code but no msg data means no new auctions
		if len(msg.Data) == 0 {
			continue
		}

		validatedResponseItems[k] = msg
	}

	// formatting the response-items as tuples for processing
	tuples, err := bus.NewRegionRealmTimestampTuplesFromMessages(validatedResponseItems)
	if err != nil {
		return err
	}

	// producing a message for computation
	data, err := tuples.EncodeForDelivery()
	if err != nil {
		return err
	}
	msg := bus.NewMessage()
	msg.Data = data

	// publishing to validate-all-auctions
	if _, err := busClient.Publish(validateAllAuctionsTopic, msg); err != nil {
		return err
	}

	return nil
}
