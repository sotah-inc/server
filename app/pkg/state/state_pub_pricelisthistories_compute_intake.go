package state

import (
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/metric/kinds"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func newPricelistHistoriesComputeIntakeRequest(data []byte) (PricelistHistoriesComputeIntakeRequest, error) {
	pRequest := &PricelistHistoriesComputeIntakeRequest{}
	err := json.Unmarshal(data, &pRequest)
	if err != nil {
		return PricelistHistoriesComputeIntakeRequest{}, err
	}

	return *pRequest, nil
}

type PricelistHistoriesComputeIntakeRequest struct {
	RegionName                string `json:"region_name"`
	RealmSlug                 string `json:"realm_slug"`
	NormalizedTargetTimestamp int    `json:"normalized_target_timestamp"`
}

func (pRequest PricelistHistoriesComputeIntakeRequest) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"region":                      pRequest.RegionName,
		"realm":                       pRequest.RealmSlug,
		"normalized-target-timestamp": pRequest.NormalizedTargetTimestamp,
	}
}

func (pRequest PricelistHistoriesComputeIntakeRequest) handle(pubState PubState, loadInJobs chan database.PricelistHistoryDatabaseV2LoadInJob) error {
	entry := logging.WithFields(logrus.Fields{
		"region_name":                 pRequest.RegionName,
		"realm_slug":                  pRequest.RealmSlug,
		"normalized_target_timestamp": pRequest.NormalizedTargetTimestamp,
	})

	entry.Info("Handling request")

	// resolving the realm from the request
	realm, err := func() (sotah.Realm, error) {
		for regionName, status := range pubState.Statuses {
			if regionName != blizzard.RegionName(pRequest.RegionName) {
				continue
			}

			for _, realm := range status.Realms {
				if realm.Slug != blizzard.RealmSlug(pRequest.RealmSlug) {
					continue
				}

				return realm, nil
			}
		}

		return sotah.Realm{}, errors.New("realm not found")
	}()
	if err != nil {
		return err
	}

	// resolving the bucket
	bkt := pubState.PricelistHistoriesBase.GetBucket(realm)
	exists, err := pubState.PricelistHistoriesBase.BucketExists(bkt)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("bucket does not exist")
	}

	// resolving the object
	obj := pubState.PricelistHistoriesBase.GetObject(time.Unix(int64(pRequest.NormalizedTargetTimestamp), 0), bkt)
	exists, err = pubState.PricelistHistoriesBase.ObjectExists(obj)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("obj does not exist")
	}

	// gathering the data from the object
	reader, err := obj.NewReader(pubState.IO.StoreClient.Context)
	if err != nil {
		return err
	}
	defer reader.Close()

	data := map[blizzard.ItemID][]byte{}
	r := csv.NewReader(reader)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		itemIdInt, err := strconv.Atoi(record[0])
		if err != nil {
			return err
		}
		itemId := blizzard.ItemID(itemIdInt)

		base64DecodedPriceHistory, err := base64.StdEncoding.DecodeString(record[1])
		if err != nil {
			return err
		}

		data[itemId] = base64DecodedPriceHistory
	}

	entry.Info("Handled request and passing into loader")

	// loading the request information and data into the pricelisthistory-database-v2
	loadInJobs <- database.PricelistHistoryDatabaseV2LoadInJob{
		RegionName:                blizzard.RegionName(pRequest.RegionName),
		RealmSlug:                 blizzard.RealmSlug(pRequest.RealmSlug),
		NormalizedTargetTimestamp: sotah.UnixTimestamp(pRequest.NormalizedTargetTimestamp),
		Data:                      data,
	}

	return nil
}

func (pubState PubState) ListenForPricelistHistoriesComputeIntake(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// declaring a load-in channel for the pricelist-histories-v2 db and starting it up
	loadInJobs := make(chan database.PricelistHistoryDatabaseV2LoadInJob)
	loadOutJobs := pubState.IO.Databases.PricelistHistoryDatabasesV2.Load(loadInJobs)
	go func() {
		for job := range loadOutJobs {
			if job.Err != nil {
				logging.WithFields(job.ToLogrusFields()).Error("Failed to load job")

				continue
			}
		}
	}()

	// starting up a worker to handle pricelist-histories-compute-intake requests
	total := func() int {
		out := 0
		for _, status := range pubState.Statuses {
			out += len(status.Realms)
		}

		return out
	}()
	in := make(chan PricelistHistoriesComputeIntakeRequest, total)
	go func() {
		for pRequest := range in {
			if err := pRequest.handle(pubState, loadInJobs); err != nil {
				logging.WithField(
					"error", err.Error(),
				).WithFields(
					pRequest.ToLogrusFields(),
				).Error("Failed to handle pricelisthistories-compute-intake request")
			}
		}
	}()

	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			// resolving the request
			pRequest, err := newPricelistHistoriesComputeIntakeRequest([]byte(busMsg.Data))
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to parse pricelist-histories-compute-intake-request")

				return
			}

			pubState.IO.Reporter.ReportWithPrefix(metric.Metrics{
				"buffer_size": len(in),
			}, kinds.PricelistHistoriesComputeIntake)
			logging.WithField("capacity", len(in)).Info("Received pricelist-histories-compute-intake-request, pushing onto handle channel")

			in <- pRequest
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := pubState.IO.BusClient.SubscribeToTopic(string(subjects.PricelistHistoriesComputeIntake), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
