package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ihsw/sotah-server/app/util"
	log "github.com/sirupsen/logrus"
)

type getAuctionsWhitelist map[realmSlug]interface{}

type getAuctionsJob struct {
	err      error
	realm    realm
	auctions *auctions
}

type realms []realm

func (reas realms) getAuctionsOrAll(res resolver, whitelist getAuctionsWhitelist) chan getAuctionsJob {
	if whitelist == nil {
		return reas.getAllAuctions(res)
	}

	return reas.getAuctions(res, whitelist)
}

func (reas realms) getAllAuctions(res resolver) chan getAuctionsJob {
	whitelist := map[realmSlug]interface{}{}
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

func newRealmFromFilepath(reg region, relativeFilepath string) (*realm, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return nil, err
	}

	return newRealm(reg, body)
}

func newRealm(reg region, body []byte) (*realm, error) {
	rea := &realm{}
	if err := json.Unmarshal(body, &rea); err != nil {
		return nil, err
	}

	rea.region = reg
	return rea, nil
}

type realmSlug string

type realm struct {
	Type            string      `json:"type"`
	Population      string      `json:"population"`
	Queue           bool        `json:"queue"`
	Status          bool        `json:"status"`
	Name            string      `json:"name"`
	Slug            realmSlug   `json:"slug"`
	Battlegroup     string      `json:"battlegroup"`
	Locale          string      `json:"locale"`
	Timezone        string      `json:"timezone"`
	ConnectedRealms []realmSlug `json:"connected_realms"`

	region region
}

func (rea realm) LogEntry() *log.Entry {
	return log.WithFields(log.Fields{"region": rea.region.Name, "realm": rea.Slug})
}

func (rea realm) getAuctions(res resolver) (*auctions, error) {
	aucInfo, err := newAuctionInfoFromHTTP(rea, res)
	if err != nil {
		return nil, err
	}

	if len(aucInfo.Files) == 0 {
		return nil, errors.New("Cannot fetch auctions with blank files")
	}
	af := aucInfo.Files[0]

	if res.config == nil {
		return nil, errors.New("Config cannot be nil")
	}

	if res.config.UseCacheDir == false {
		return newAuctionsFromHTTP(af.URL, res)
	}

	if res.config.CacheDir == "" {
		return nil, errors.New("Cache dir cannot be blank")
	}

	if rea.region.Name == "" {
		return nil, errors.New("Region name cannot be blank")
	}

	auctionsFilepath, err := filepath.Abs(
		fmt.Sprintf("%s/auctions/%s/%s.json.gz", res.config.CacheDir, rea.region.Name, rea.Slug),
	)
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(auctionsFilepath); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		body, err := res.get(res.getAuctionsURL(af.URL))
		if err != nil {
			return nil, err
		}

		encodedBody, err := util.GzipEncode(body)
		if err != nil {
			return nil, err
		}

		log.WithFields(log.Fields{
			"region": rea.region.Name,
			"realm":  rea.Slug,
		}).Debug("Writing auction data to cache dir")
		if err := util.WriteFile(auctionsFilepath, encodedBody); err != nil {
			return nil, err
		}

		return newAuctions(body)
	}

	rea.LogEntry().Debug("Loading auction data from cache dir")
	return newAuctionsFromGzFilepath(rea, auctionsFilepath)
}
