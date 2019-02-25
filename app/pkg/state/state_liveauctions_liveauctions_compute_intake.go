package state

import (
	"encoding/json"
	"errors"
	"io/ioutil"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/metric/kinds"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
)

func newLiveAuctionsComputeIntakeRequest(data []byte) (LiveAuctionsComputeIntakeRequest, error) {
	pRequest := &LiveAuctionsComputeIntakeRequest{}
	err := json.Unmarshal(data, &pRequest)
	if err != nil {
		return LiveAuctionsComputeIntakeRequest{}, err
	}

	return *pRequest, nil
}

type LiveAuctionsComputeIntakeRequest struct {
	RegionName string `json:"region_name"`
	RealmSlug  string `json:"realm_slug"`
}

func (cRequest LiveAuctionsComputeIntakeRequest) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"region": cRequest.RegionName,
		"realm":  cRequest.RealmSlug,
	}
}

func (cRequest LiveAuctionsComputeIntakeRequest) handle(laState LiveAuctionsState, loadInJobs chan database.LiveAuctionsLoadEncodedDataInJob) error {
	entry := logging.WithFields(logrus.Fields{
		"region_name": cRequest.RegionName,
		"realm_slug":  cRequest.RealmSlug,
	})

	entry.Info("Handling request")

	// resolving the realm from the request
	realm, err := func() (sotah.Realm, error) {
		for regionName, status := range laState.Statuses {
			if regionName != blizzard.RegionName(cRequest.RegionName) {
				continue
			}

			for _, realm := range status.Realms {
				if realm.Slug != blizzard.RealmSlug(cRequest.RealmSlug) {
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
	bkt := laState.LiveAuctionsBase.GetBucket()
	exists, err := laState.LiveAuctionsBase.BucketExists(bkt)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("bucket does not exist")
	}

	// resolving the object
	obj := laState.LiveAuctionsBase.GetObject(realm, bkt)
	exists, err = laState.LiveAuctionsBase.ObjectExists(obj)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("obj does not exist")
	}

	// gathering the data from the object
	reader, err := obj.NewReader(laState.IO.StoreClient.Context)
	if err != nil {
		return err
	}
	defer reader.Close()

	// re-encoding it because gcloud storage client automatically gzip-decodes
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	encodedData, err := util.GzipEncode(data)
	if err != nil {
		return err
	}

	entry.Info("Handled request and passing into loader")

	// loading the request information and data into the live-auctions-database
	loadInJobs <- database.LiveAuctionsLoadEncodedDataInJob{
		RegionName:  blizzard.RegionName(cRequest.RegionName),
		RealmSlug:   blizzard.RealmSlug(cRequest.RealmSlug),
		EncodedData: encodedData,
	}

	return nil
}

func (laState LiveAuctionsState) ListenForLiveAuctionsComputeIntake(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// declaring a load-in channel for the live-auctions db and starting it up
	loadInJobs := make(chan database.LiveAuctionsLoadEncodedDataInJob)
	loadOutJobs := laState.IO.Databases.LiveAuctionsDatabases.LoadEncodedData(loadInJobs)
	go func() {
		for job := range loadOutJobs {
			if job.Err != nil {
				logging.WithFields(job.ToLogrusFields()).Error("Failed to load job")

				continue
			}

			logging.WithFields(logrus.Fields{
				"region": job.RegionName,
				"realm":  job.RealmSlug,
			}).Info("Loaded job")
		}
	}()

	// starting up workers to handle live-auctions-compute-intake requests
	total := func() int {
		out := 0
		for _, status := range laState.Statuses {
			out += len(status.Realms)
		}

		return out
	}()
	in := make(chan LiveAuctionsComputeIntakeRequest, total)
	worker := func() {
		for cRequest := range in {
			if err := cRequest.handle(laState, loadInJobs); err != nil {
				logging.WithField(
					"error", err.Error(),
				).WithFields(
					cRequest.ToLogrusFields(),
				).Error("Failed to handle live-auctions-compute-intake request")
			}
		}
	}
	postWork := func() {
		close(loadInJobs)
	}
	util.Work(4, worker, postWork)

	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			// resolving the request
			pRequest, err := newLiveAuctionsComputeIntakeRequest([]byte(busMsg.Data))
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to parse live-auctions-compute-intake-request")

				return
			}

			laState.IO.Reporter.ReportWithPrefix(metric.Metrics{
				"buffer_size": len(in),
			}, kinds.LiveAuctionsComputeIntake)
			logging.WithField("capacity", len(in)).Info("Received live-auctions-compute-intake-request, pushing onto handle channel")

			in <- pRequest
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := laState.IO.BusClient.SubscribeToTopic(string(subjects.LiveAuctionsComputeIntake), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
