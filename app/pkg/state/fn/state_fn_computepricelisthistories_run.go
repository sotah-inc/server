package fn

import (
	"io/ioutil"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
)

func (sta ComputeLivePricelistHistoriesState) Handle(job bus.LoadRegionRealmTimestampsInJob) bus.Message {
	m := bus.NewMessage()

	realm, targetTime := job.ToRealmTime()

	obj, err := sta.auctionsStoreBase.GetFirmObject(realm, targetTime, sta.auctionsBucket)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	reader, err := obj.NewReader(sta.IO.StoreClient.Context)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	aucs, err := blizzard.NewAuctions(data)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	logging.WithFields(logrus.Fields{
		"region":        realm.Region.Name,
		"realm":         realm.Slug,
		"last-modified": targetTime.Unix(),
	}).Info("Parsed into live-auctions, handling pricelist-history")
	normalizedTargetTimestamp, err := sta.pricelistHistoriesStoreBase.Handle(
		aucs,
		targetTime,
		realm,
		sta.pricelistHistoriesBucket,
	)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	logging.WithField("normalized-target-timestamp", normalizedTargetTimestamp).Info("e")

	return m
}

func (sta ComputeLivePricelistHistoriesState) Run() error {
	return nil
}
