package fn

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	bCodes "github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func (sta DownloadAllAuctionsState) PublishToReceiveRealms(
	regionRealmMap sotah.RegionRealmMap,
	tuples bus.RegionRealmTimestampTuples,
) error {
	// gathering a whitelist of region-realm-slugs
	regionRealmSlugs := tuples.ToRegionRealmSlugs()

	// gathering hell-realms for syncing
	hellRegionRealms, err := sta.IO.HellClient.GetRegionRealms(regionRealmSlugs, gameversions.Retail)
	if err != nil {
		return err
	}

	// updating the list of realms' timestamps
	for _, tuple := range tuples {
		hellRealm := hellRegionRealms[blizzard.RegionName(tuple.RegionName)][blizzard.RealmSlug(tuple.RealmSlug)]
		hellRealm.Downloaded = tuple.TargetTimestamp
		hellRegionRealms[blizzard.RegionName(tuple.RegionName)][blizzard.RealmSlug(tuple.RealmSlug)] = hellRealm
	}
	if err := sta.IO.HellClient.WriteRegionRealms(hellRegionRealms, gameversions.Retail); err != nil {
		return err
	}

	jsonEncoded, err := json.Marshal(regionRealmSlugs)
	if err != nil {
		return err
	}

	req, err := sta.IO.Messenger.Request(string(subjects.ReceiveRealms), jsonEncoded)
	if err != nil {
		return err
	}

	if req.Code != mCodes.Ok {
		return errors.New(req.Err)
	}

	return nil
}

func (sta DownloadAllAuctionsState) Run() error {
	regions, err := sta.bootBase.GetRegions(sta.bootBucket)
	if err != nil {
		return err
	}

	regionRealmMap := sotah.RegionRealmMap{}
	for _, region := range regions {
		realms, err := sta.realmsBase.GetAllRealms(region.Name, sta.realmsBucket)
		if err != nil {
			return err
		}

		regionRealmMap[region.Name] = realms.ToRealmMap()
	}

	// producing messages
	logging.Info("Producing messages for bulk requesting")
	messages, err := bus.NewCollectAuctionMessages(regionRealmMap.ToRegionRealms())
	if err != nil {
		return err
	}

	// enqueueing them and gathering result jobs
	startTime := time.Now()
	responseItems, err := sta.IO.BusClient.BulkRequest(sta.downloadAuctionsTopic, messages, 120*time.Second)
	if err != nil {
		return err
	}

	// gathering validated response messages
	validatedResponseItems := bus.BulkRequestMessages{}
	for k, msg := range responseItems {
		if msg.Code != bCodes.Ok {
			if msg.Code == bCodes.BlizzardError {
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

	// reporting metrics
	if err := sta.IO.BusClient.PublishMetrics(metric.Metrics{
		"download_all_auctions_duration": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000),
		"included_realms":                len(validatedResponseItems),
	}); err != nil {
		return err
	}

	// formatting the response-items as tuples for processing
	tuples, err := bus.NewRegionRealmTimestampTuplesFromMessages(validatedResponseItems)
	if err != nil {
		return err
	}

	// publishing to receive-realms
	logging.Info("Publishing realms to receive-realms")
	if err := sta.PublishToReceiveRealms(regionRealmMap, tuples); err != nil {
		return err
	}

	// encoding tuples for publishing to compute-all-live-auctions and compute-all-pricelist-histories
	encodedTuples, err := tuples.EncodeForDelivery()
	if err != nil {
		return err
	}
	encodedTuplesMsg := bus.NewMessage()
	encodedTuplesMsg.Data = encodedTuples

	// publishing to compute-all-live-auctions
	logging.WithField("tuples", len(tuples)).Info("Publishing to compute-all-live-auctions")
	if _, err := sta.IO.BusClient.Publish(sta.computeAllLiveAuctionsTopic, encodedTuplesMsg); err != nil {
		return err
	}

	// publishing to compute-all-pricelist-histories
	logging.Info("Publishing to compute-all-live-auctions")
	if _, err := sta.IO.BusClient.Publish(sta.computeAllPricelistHistoriesTopic, encodedTuplesMsg); err != nil {
		return err
	}

	logging.Info("Finished")

	return nil
}
