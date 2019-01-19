package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/blizzard"
	"github.com/sotah-inc/server/app/codes"
	"github.com/sotah-inc/server/app/logging"
	"github.com/sotah-inc/server/app/subjects"
	"github.com/sotah-inc/server/app/util"
)

type getAuctionsWhitelist map[blizzard.RealmSlug]interface{}

func newRealms(reg region, blizzRealms []blizzard.Realm) realms {
	reas := make([]realm, len(blizzRealms))
	for i, rea := range blizzRealms {
		reas[i] = realm{rea, reg, 0}
	}

	return reas
}

type getAuctionsJob struct {
	err          error
	realm        realm
	auctions     blizzard.Auctions
	lastModified time.Time
}

type realms []realm

func (reas realms) filterWithWhitelist(wList *getAuctionsWhitelist) realms {
	if wList == nil {
		return reas
	}

	wListValue := *wList

	out := realms{}
	for _, rea := range reas {
		if _, ok := wListValue[rea.Slug]; !ok {
			continue
		}

		out = append(out, rea)
	}

	return out
}

func (reas realms) getAuctionsOrAll(res resolver, wList *getAuctionsWhitelist) chan getAuctionsJob {
	if wList == nil {
		return reas.getAllAuctions(res)
	}

	return reas.getAuctions(res, *wList)
}

func (reas realms) getAllAuctions(res resolver) chan getAuctionsJob {
	wList := getAuctionsWhitelist{}
	for _, rea := range reas {
		wList[rea.Slug] = true
	}
	return reas.getAuctions(res, wList)
}

func (reas realms) getAuctions(res resolver, wList getAuctionsWhitelist) chan getAuctionsJob {
	// establishing channels
	out := make(chan getAuctionsJob)
	in := make(chan realm)

	// spinning up the workers for fetching auctions
	worker := func() {
		for rea := range in {
			aucs, lastModified, err := rea.getAuctions(res)

			// optionally skipping draining out due to error
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": rea.region.Name,
					"realm":  rea.Slug,
				}).Error("Auction fetch failure")

				continue
			}

			// optionally skipping draining out due to no new data
			if lastModified.IsZero() {
				logging.WithFields(logrus.Fields{
					"region": rea.region.Name,
					"realm":  rea.Slug,
				}).Info("No auctions received")

				continue
			}

			// draining out
			logging.WithFields(logrus.Fields{
				"region":   rea.region.Name,
				"realm":    rea.Slug,
				"auctions": len(aucs.Auctions),
			}).Debug("Auctions received")
			out <- getAuctionsJob{nil, rea, aucs, lastModified}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the realms
	go func() {
		for _, rea := range reas {
			if _, ok := wList[rea.Slug]; !ok {
				continue
			}

			logging.WithFields(logrus.Fields{
				"region": rea.region.Name,
				"realm":  rea.Slug,
			}).Debug("Queueing up auction for downloading")
			in <- rea
		}

		close(in)
	}()

	return out
}

func (rea realm) databaseDir(parentDirPath string) string {
	return fmt.Sprintf("%s/%s", parentDirPath, rea.Slug)
}

type loadAuctionsJob struct {
	err          error
	realm        realm
	auctions     blizzard.Auctions
	lastModified time.Time
}

func (reas realms) loadAuctions(c *config, sto store) chan loadAuctionsJob {
	if c.UseGCloud {
		return sto.loadRealmsAuctions(c, reas)
	}

	return reas.loadAuctionsFromCacheDir(c)
}

func (reas realms) loadAuctionsFromCacheDir(c *config) chan loadAuctionsJob {
	// establishing channels
	out := make(chan loadAuctionsJob)
	in := make(chan realm)

	// spinning up the workers for fetching auctions
	worker := func() {
		for rea := range in {
			aucs, lastModified, err := rea.loadAuctionsFromFilecache(c)
			if lastModified.IsZero() {
				logging.WithFields(logrus.Fields{
					"region": rea.region.Name,
					"realm":  rea.Slug,
				}).Error("Last-modified was blank when loading auctions from filecache")

				continue
			}

			out <- loadAuctionsJob{err, rea, aucs, lastModified}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the realms
	go func() {
		for _, rea := range reas {
			wList := c.getRegionWhitelist(rea.region.Name)
			if wList != nil {
				resolvedWhiteList := *wList
				if _, ok := resolvedWhiteList[rea.Slug]; !ok {
					continue
				}
			}

			logging.WithFields(logrus.Fields{
				"region": rea.region.Name,
				"realm":  rea.Slug,
			}).Debug("Queueing up auction for loading")
			in <- rea
		}

		close(in)
	}()

	return out
}

type realm struct {
	blizzard.Realm
	region       region
	LastModified int64 `json:"last_modified"`
}

func (rea realm) LogEntry() *logrus.Entry {
	return logging.WithFields(logrus.Fields{"region": rea.region.Name, "realm": rea.Slug})
}

func (rea realm) auctionsFilepath(c *config) (string, error) {
	return filepath.Abs(
		fmt.Sprintf("%s/auctions/%s/%s.json.gz", c.CacheDir, rea.region.Name, rea.Slug),
	)
}

func (rea realm) getAuctions(res resolver) (blizzard.Auctions, time.Time, error) {
	uri, err := res.appendAccessToken(res.getAuctionInfoURL(rea.region.Hostname, rea.Slug))
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}

	// resolving auction-info from the api
	aInfo, _, err := blizzard.NewAuctionInfoFromHTTP(uri)
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}

	// validating the list of files
	if len(aInfo.Files) == 0 {
		return blizzard.Auctions{}, time.Time{}, errors.New("Cannot fetch auctions with blank files")
	}
	aFile := aInfo.Files[0]

	// validating the realm region
	if rea.region.Name == "" {
		return blizzard.Auctions{}, time.Time{}, errors.New("Region name cannot be blank")
	}

	// optionally downloading where the realm has stale data
	if rea.LastModified == 0 || time.Unix(rea.LastModified, 0).Before(aFile.LastModifiedAsTime()) {
		aucs, err := rea.downloadAndCache(aFile, res)
		if err != nil {
			return blizzard.Auctions{}, time.Time{}, err
		}

		return aucs, aFile.LastModifiedAsTime(), nil
	}

	return blizzard.Auctions{}, time.Time{}, nil
}

func (rea realm) downloadAndCache(aFile blizzard.AuctionFile, res resolver) (blizzard.Auctions, error) {
	// validating config
	if res.config == nil {
		return blizzard.Auctions{}, errors.New("Config cannot be nil")
	}

	// downloading the auction data
	resp, err := blizzard.Download(aFile.URL)
	if err != nil {
		return blizzard.Auctions{}, err
	}

	// gathering the encoded body
	encodedBody, err := util.GzipEncode(resp.Body)

	if res.config.UseGCloud {
		logging.WithFields(logrus.Fields{
			"region":       rea.region.Name,
			"realm":        rea.Slug,
			"lastModified": aFile.LastModifiedAsTime().Unix(),
			"encodedBody":  len(encodedBody),
		}).Debug("Writing auction data to gcloud store")

		// writing the auction data to the gcloud storage
		if err := res.store.writeRealmAuctions(rea, aFile.LastModifiedAsTime(), encodedBody); err != nil {
			logging.WithFields(logrus.Fields{
				"error":        err.Error(),
				"region":       rea.region.Name,
				"realm":        rea.Slug,
				"lastModified": aFile.LastModifiedAsTime().Unix(),
			}).Debug("Failed to write auctions to gcloud storage")

			return blizzard.Auctions{}, err
		}

		return blizzard.NewAuctions(resp.Body)
	}

	// validating config
	if res.config.CacheDir == "" {
		return blizzard.Auctions{}, errors.New("Cache dir cannot be blank")
	}

	// gathering auctions filepath
	auctionsFilepath, err := rea.auctionsFilepath(res.config)
	if err != nil {
		return blizzard.Auctions{}, err
	}

	// writing the auction data to the cache dir
	logging.WithFields(logrus.Fields{
		"region":      rea.region.Name,
		"realm":       rea.Slug,
		"filepath":    auctionsFilepath,
		"encodedBody": len(encodedBody),
	}).Debug("Writing auction data to cache dir")
	if err != nil {
		return blizzard.Auctions{}, err
	}
	if err := util.WriteFile(auctionsFilepath, encodedBody); err != nil {
		return blizzard.Auctions{}, err
	}

	return blizzard.NewAuctions(resp.Body)
}

func (rea realm) loadAuctionsFromFilecache(c *config) (blizzard.Auctions, time.Time, error) {
	// resolving the cached auctions filepath
	cachedAuctionsFilepath, err := rea.auctionsFilepath(c)
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}

	// optionally skipping non-exist auctions files
	cachedAuctionsStat, err := os.Stat(cachedAuctionsFilepath)
	if err != nil {
		if !os.IsNotExist(err) {
			return blizzard.Auctions{}, time.Time{}, err
		}

		return blizzard.Auctions{}, time.Time{}, nil
	}

	// loading the gzipped cached auctions file
	logging.WithFields(logrus.Fields{
		"region":   rea.region.Name,
		"realm":    rea.Slug,
		"filepath": cachedAuctionsFilepath,
	}).Debug("Loading auctions from filepath")
	aucs, err := blizzard.NewAuctionsFromGzFilepath(cachedAuctionsFilepath)
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}
	logging.WithFields(logrus.Fields{
		"region":   rea.region.Name,
		"realm":    rea.Slug,
		"filepath": cachedAuctionsFilepath,
	}).Debug("Finished loading auctions from filepath")

	return aucs, cachedAuctionsStat.ModTime(), nil
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
	Realms realms `json:"realms"`
}

type statuses map[regionName]status
