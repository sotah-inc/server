package fn

import (
	"encoding/json"
	"io/ioutil"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
)

func (sta ComputeLiveAuctionsState) Handle(job bus.LoadRegionRealmTimestampsInJob) bus.Message {
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
	}).Info("Parsing into live-auctions")
	if err := sta.liveAuctionsStoreBase.Handle(aucs, realm, sta.liveAuctionsBucket); err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	replyTuple := bus.RegionRealmTimestampTuple{
		RegionName:      job.RegionName,
		RealmSlug:       job.RealmSlug,
		TargetTimestamp: job.TargetTimestamp,
		ItemIds:         aucs.ItemIds().ToInts(),
		OwnerNames:      aucs.OwnerNames(),
	}
	encodedReplyTuple, err := replyTuple.EncodeForDelivery()
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}
	m.Data = encodedReplyTuple

	return m
}

func (sta ComputeLiveAuctionsState) Run(data string) error {
	var in bus.Message
	if err := json.Unmarshal([]byte(data), &in); err != nil {
		return err
	}

	job, err := bus.NewLoadRegionRealmTimestampsInJob(in.Data)
	if err != nil {
		return err
	}

	msg := sta.Handle(job)
	msg.ReplyToId = in.ReplyToId
	if _, err := sta.IO.BusClient.ReplyTo(in, msg); err != nil {
		return err
	}

	return nil
}
