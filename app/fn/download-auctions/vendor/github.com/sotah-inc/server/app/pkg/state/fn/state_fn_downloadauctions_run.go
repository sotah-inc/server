package fn

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func (sta DownloadAuctionsState) ResolveRealm(job bus.CollectAuctionsJob) (sotah.Realm, error) {
	region, err := func() (sotah.Region, error) {
		for _, reg := range sta.regions {
			if reg.Name == blizzard.RegionName(job.RegionName) {
				return reg, nil
			}
		}

		return sotah.Region{}, errors.New("could not resolve region from job")
	}()
	if err != nil {
		return sotah.Realm{}, err
	}

	realm := sotah.NewSkeletonRealm(blizzard.RegionName(job.RegionName), blizzard.RealmSlug(job.RealmSlug))
	realm.Region = region

	return realm, nil
}

func (sta DownloadAuctionsState) Handle(job bus.CollectAuctionsJob) bus.Message {
	m := bus.NewMessage()

	realm, err := sta.ResolveRealm(job)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	uri, err := sta.blizzardClient.AppendAccessToken(blizzard.DefaultGetAuctionInfoURL(
		realm.Region.Hostname,
		blizzard.RealmSlug(job.RealmSlug),
	))
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

	obj := sta.auctionsStoreBase.GetObject(realm, lastModifiedTime, sta.auctionsBucket)
	exists, err := sta.auctionsStoreBase.ObjectExists(obj)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}
	if exists {
		logging.WithFields(logrus.Fields{
			"region":        realm.Region.Name,
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
		"region":        realm.Region.Name,
		"realm":         realm.Slug,
		"last-modified": lastModifiedTimestamp,
	}).Info("Parsed, saving to raw-auctions store")
	if err := sta.auctionsStoreBase.Handle(resp.Body, lastModifiedTime, realm, sta.auctionsBucket); err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	logging.WithFields(logrus.Fields{
		"region":        realm.Region.Name,
		"realm":         realm.Slug,
		"last-modified": lastModifiedTimestamp,
	}).Info("Saved, adding to auction-manifest file")
	if err := sta.auctionManifestStoreBase.Handle(lastModifiedTimestamp, realm, sta.auctionsManifestBucket); err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	replyTuple := bus.RegionRealmTimestampTuple{
		RegionName:      job.RegionName,
		RealmSlug:       job.RealmSlug,
		TargetTimestamp: int(lastModifiedTimestamp),
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

func (sta DownloadAuctionsState) Run(data string) error {
	var in bus.Message
	if err := json.Unmarshal([]byte(data), &in); err != nil {
		return err
	}

	var job bus.CollectAuctionsJob
	if err := json.Unmarshal([]byte(in.Data), &job); err != nil {
		return err
	}

	msg := sta.Handle(job)
	msg.ReplyToId = in.ReplyToId
	if _, err := sta.IO.BusClient.ReplyTo(in, msg); err != nil {
		return err
	}

	return nil
}
