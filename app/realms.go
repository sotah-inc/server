package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
	log "github.com/sirupsen/logrus"
)

type getAuctionsWhitelist map[blizzard.RealmSlug]interface{}

type getAuctionsJob struct {
	err      error
	realm    realm
	auctions auctions
}

func newRealms(reg region, blizzRealms []blizzard.Realm) realms {
	reas := make([]realm, len(blizzRealms))
	for i, rea := range blizzRealms {
		reas[i] = realm{rea, reg}
	}

	return reas
}

type realms []realm

func (reas realms) getAuctionsOrAll(res resolver, whitelist getAuctionsWhitelist) chan getAuctionsJob {
	if whitelist == nil {
		return reas.getAllAuctions(res)
	}

	return reas.getAuctions(res, whitelist)
}

func (reas realms) getAllAuctions(res resolver) chan getAuctionsJob {
	whitelist := map[blizzard.RealmSlug]interface{}{}
	for _, rea := range reas {
		whitelist[rea.Slug] = true
	}
	return reas.getAuctions(res, whitelist)
}

func (reas realms) getAuctions(res resolver, whitelist getAuctionsWhitelist) chan getAuctionsJob {
	// establishing channels
	out := make(chan getAuctionsJob)
	in := make(chan realm)

	// spinning up the workers for fetching auctions
	worker := func() {
		for rea := range in {
			aucs, err := rea.getAuctions(res)
			log.WithField("realm", rea.Slug).Debug("Received auctions")
			out <- getAuctionsJob{err: err, realm: rea, auctions: aucs}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the realms
	go func() {
		for _, rea := range reas {
			if _, ok := whitelist[rea.Slug]; !ok {
				continue
			}

			log.WithField("realm", rea.Slug).Debug("Queueing up auction for downloading")
			in <- rea
		}

		close(in)
	}()

	return out
}

type realm struct {
	blizzard.Realm
	region region
}

func (rea realm) LogEntry() *log.Entry {
	return log.WithFields(log.Fields{"region": rea.region.Name, "realm": rea.Slug})
}

func (rea realm) getAuctions(res resolver) (auctions, error) {
	aucInfo, err := newAuctionInfoFromHTTP(rea, res)
	if err != nil {
		return auctions{}, err
	}

	if len(aucInfo.Files) == 0 {
		return auctions{}, errors.New("Cannot fetch auctions with blank files")
	}
	af := aucInfo.Files[0]

	if res.config == nil {
		return auctions{}, errors.New("Config cannot be nil")
	}

	if res.config.UseCacheDir == false {
		return newAuctionsFromHTTP(af.URL, res)
	}

	if res.config.CacheDir == "" {
		return auctions{}, errors.New("Cache dir cannot be blank")
	}

	if rea.region.Name == "" {
		return auctions{}, errors.New("Region name cannot be blank")
	}

	auctionsFilepath, err := filepath.Abs(
		fmt.Sprintf("%s/auctions/%s/%s.json.gz", res.config.CacheDir, rea.region.Name, rea.Slug),
	)
	if err != nil {
		return auctions{}, err
	}

	if _, err := os.Stat(auctionsFilepath); err != nil {
		if !os.IsNotExist(err) {
			return auctions{}, err
		}

		body, err := res.get(res.getAuctionsURL(af.URL))
		if err != nil {
			return auctions{}, err
		}

		encodedBody, err := util.GzipEncode(body)
		if err != nil {
			return auctions{}, err
		}

		log.WithFields(log.Fields{
			"region": rea.region.Name,
			"realm":  rea.Slug,
		}).Debug("Writing auction data to cache dir")
		if err := util.WriteFile(auctionsFilepath, encodedBody); err != nil {
			return auctions{}, err
		}

		return newAuctions(body)
	}

	rea.LogEntry().Debug("Loading auction data from cache dir")
	return newAuctionsFromGzFilepath(rea, auctionsFilepath)
}

func newStatusFromMessenger(reg region, mess messenger) (status, error) {
	lm := statusRequest{RegionName: reg.Name}
	encodedMessage, err := json.Marshal(lm)
	if err != nil {
		return status{}, err
	}

	msg, err := mess.request(subjects.Status, encodedMessage)
	if err != nil {
		return status{}, err
	}

	if msg.Code != codes.Ok {
		return status{}, errors.New(msg.Err)
	}

	stat, err := blizzard.NewStatus([]byte(msg.Data))
	if err != nil {
		return status{}, err
	}

	return newStatus(reg, stat), nil
}

func newStatusFromFilepath(reg region, relativeFilepath string) (status, error) {
	stat, err := blizzard.NewStatusFromFilepath(relativeFilepath)
	if err != nil {
		return status{}, err
	}

	return newStatus(reg, stat), nil
}

func newStatus(reg region, stat blizzard.Status) status {
	return status{stat, reg, newRealms(reg, stat.Realms)}
}

type status struct {
	blizzard.Status
	region region
	Realms realms
}
