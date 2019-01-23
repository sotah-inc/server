package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
)

type getAuctionsWhitelist map[blizzard.RealmSlug]interface{}

func newRealms(reg Region, blizzRealms []blizzard.Realm) Realms {
	reas := make([]Realm, len(blizzRealms))
	for i, rea := range blizzRealms {
		reas[i] = Realm{rea, reg, 0}
	}

	return reas
}

type getAuctionsJob struct {
	err          error
	realm        Realm
	auctions     blizzard.Auctions
	lastModified time.Time
}

type Realms []Realm

func (reas Realms) FilterWithWhitelist(wList *getAuctionsWhitelist) Realms {
	if wList == nil {
		return reas
	}

	wListValue := *wList

	out := Realms{}
	for _, rea := range reas {
		if _, ok := wListValue[rea.Slug]; !ok {
			continue
		}

		out = append(out, rea)
	}

	return out
}

func (reas Realms) getAuctionsOrAll(res Resolver, wList *getAuctionsWhitelist) chan getAuctionsJob {
	if wList == nil {
		return reas.getAllAuctions(res)
	}

	return reas.getAuctions(res, *wList)
}

func (reas Realms) getAllAuctions(res Resolver) chan getAuctionsJob {
	wList := getAuctionsWhitelist{}
	for _, rea := range reas {
		wList[rea.Slug] = true
	}
	return reas.getAuctions(res, wList)
}

func (reas Realms) getAuctions(res Resolver, wList getAuctionsWhitelist) chan getAuctionsJob {
	// establishing channels
	out := make(chan getAuctionsJob)
	in := make(chan Realm)

	// spinning up the workers for fetching Auctions
	worker := func() {
		for rea := range in {
			aucs, lastModified, err := rea.getAuctions(res)

			// optionally skipping draining out due to error
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"Region": rea.Region.Name,
					"Realm":  rea.Slug,
				}).Error("Auction fetch failure")

				continue
			}

			// optionally skipping draining out due to no new data
			if lastModified.IsZero() {
				logging.WithFields(logrus.Fields{
					"Region": rea.Region.Name,
					"Realm":  rea.Slug,
				}).Info("No Auctions received")

				continue
			}

			// draining out
			logging.WithFields(logrus.Fields{
				"Region":   rea.Region.Name,
				"Realm":    rea.Slug,
				"Auctions": len(aucs.Auctions),
			}).Debug("Auctions received")
			out <- getAuctionsJob{nil, rea, aucs, lastModified}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the Realms
	go func() {
		for _, rea := range reas {
			if _, ok := wList[rea.Slug]; !ok {
				continue
			}

			logging.WithFields(logrus.Fields{
				"Region": rea.Region.Name,
				"Realm":  rea.Slug,
			}).Debug("Queueing up auction for downloading")
			in <- rea
		}

		close(in)
	}()

	return out
}

func (rea Realm) DatabaseDir(parentDirPath string) string {
	return fmt.Sprintf("%s/%s", parentDirPath, rea.Slug)
}

type LoadAuctionsJob struct {
	Err          error
	Realm        Realm
	Auctions     blizzard.Auctions
	LastModified time.Time
}

func (reas Realms) loadAuctions(c *Config, sto store.Store) chan LoadAuctionsJob {
	if c.UseGCloud {
		return sto.LoadRealmsAuctions(c, reas)
	}

	return reas.LoadAuctionsFromCacheDir(c)
}

func (reas Realms) LoadAuctionsFromCacheDir(c *Config) chan LoadAuctionsJob {
	// establishing channels
	out := make(chan LoadAuctionsJob)
	in := make(chan Realm)

	// spinning up the workers for fetching Auctions
	worker := func() {
		for rea := range in {
			aucs, lastModified, err := rea.loadAuctionsFromFilecache(c)
			if lastModified.IsZero() {
				logging.WithFields(logrus.Fields{
					"Region": rea.Region.Name,
					"Realm":  rea.Slug,
				}).Error("Last-modified was blank when loading Auctions from filecache")

				continue
			}

			out <- LoadAuctionsJob{err, rea, aucs, lastModified}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the Realms
	go func() {
		for _, rea := range reas {
			wList := c.GetRegionWhitelist(rea.Region.Name)
			if wList != nil {
				resolvedWhiteList := *wList
				if _, ok := resolvedWhiteList[rea.Slug]; !ok {
					continue
				}
			}

			logging.WithFields(logrus.Fields{
				"Region": rea.Region.Name,
				"Realm":  rea.Slug,
			}).Debug("Queueing up auction for loading")
			in <- rea
		}

		close(in)
	}()

	return out
}

type Realm struct {
	blizzard.Realm
	Region       Region
	LastModified int64 `json:"last_modified"`
}

func (rea Realm) LogEntry() *logrus.Entry {
	return logging.WithFields(logrus.Fields{"Region": rea.Region.Name, "Realm": rea.Slug})
}

func (rea Realm) auctionsFilepath(c *Config) (string, error) {
	return filepath.Abs(
		fmt.Sprintf("%s/Auctions/%s/%s.json.gz", c.CacheDir, rea.Region.Name, rea.Slug),
	)
}

func (rea Realm) getAuctions(res Resolver) (blizzard.Auctions, time.Time, error) {
	uri, err := res.appendAccessToken(res.getAuctionInfoURL(rea.Region.Hostname, rea.Slug))
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
		return blizzard.Auctions{}, time.Time{}, errors.New("Cannot fetch Auctions with blank files")
	}
	aFile := aInfo.Files[0]

	// validating the Realm Region
	if rea.Region.Name == "" {
		return blizzard.Auctions{}, time.Time{}, errors.New("Region name cannot be blank")
	}

	// optionally downloading where the Realm has stale data
	if rea.LastModified == 0 || time.Unix(rea.LastModified, 0).Before(aFile.LastModifiedAsTime()) {
		aucs, err := rea.downloadAndCache(aFile, res)
		if err != nil {
			return blizzard.Auctions{}, time.Time{}, err
		}

		return aucs, aFile.LastModifiedAsTime(), nil
	}

	return blizzard.Auctions{}, time.Time{}, nil
}

func (rea Realm) downloadAndCache(aFile blizzard.AuctionFile, res Resolver) (blizzard.Auctions, error) {
	// validating Config
	if res.Config == nil {
		return blizzard.Auctions{}, errors.New("Config cannot be nil")
	}

	// downloading the auction data
	resp, err := blizzard.Download(aFile.URL)
	if err != nil {
		return blizzard.Auctions{}, err
	}

	// gathering the encoded body
	encodedBody, err := util.GzipEncode(resp.Body)

	if res.Config.UseGCloud {
		logging.WithFields(logrus.Fields{
			"Region":       rea.Region.Name,
			"Realm":        rea.Slug,
			"LastModified": aFile.LastModifiedAsTime().Unix(),
			"encodedBody":  len(encodedBody),
		}).Debug("Writing auction data to gcloud Store")

		// writing the auction data to the gcloud storage
		if err := res.Store.WriteRealmAuctions(rea, aFile.LastModifiedAsTime(), encodedBody); err != nil {
			logging.WithFields(logrus.Fields{
				"error":        err.Error(),
				"Region":       rea.Region.Name,
				"Realm":        rea.Slug,
				"LastModified": aFile.LastModifiedAsTime().Unix(),
			}).Debug("Failed to write Auctions to gcloud storage")

			return blizzard.Auctions{}, err
		}

		return blizzard.NewAuctions(resp.Body)
	}

	// validating Config
	if res.Config.CacheDir == "" {
		return blizzard.Auctions{}, errors.New("Cache dir cannot be blank")
	}

	// gathering Auctions filepath
	auctionsFilepath, err := rea.auctionsFilepath(res.Config)
	if err != nil {
		return blizzard.Auctions{}, err
	}

	// writing the auction data to the cache dir
	logging.WithFields(logrus.Fields{
		"Region":      rea.Region.Name,
		"Realm":       rea.Slug,
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

func (rea Realm) loadAuctionsFromFilecache(c *Config) (blizzard.Auctions, time.Time, error) {
	// resolving the cached Auctions filepath
	cachedAuctionsFilepath, err := rea.auctionsFilepath(c)
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}

	// optionally skipping non-exist Auctions files
	cachedAuctionsStat, err := os.Stat(cachedAuctionsFilepath)
	if err != nil {
		if !os.IsNotExist(err) {
			return blizzard.Auctions{}, time.Time{}, err
		}

		return blizzard.Auctions{}, time.Time{}, nil
	}

	// loading the gzipped cached Auctions file
	logging.WithFields(logrus.Fields{
		"Region":   rea.Region.Name,
		"Realm":    rea.Slug,
		"filepath": cachedAuctionsFilepath,
	}).Debug("Loading Auctions from filepath")
	aucs, err := blizzard.NewAuctionsFromGzFilepath(cachedAuctionsFilepath)
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}
	logging.WithFields(logrus.Fields{
		"Region":   rea.Region.Name,
		"Realm":    rea.Slug,
		"filepath": cachedAuctionsFilepath,
	}).Debug("Finished loading Auctions from filepath")

	return aucs, cachedAuctionsStat.ModTime(), nil
}

func newStatusFromMessenger(reg Region, mess messenger.Messenger) (status, error) {
	lm := state.StatusRequest{RegionName: reg.Name}
	encodedMessage, err := json.Marshal(lm)
	if err != nil {
		return status{}, err
	}

	msg, err := mess.Request(subjects.Status, encodedMessage)
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

func newStatusFromFilepath(reg Region, relativeFilepath string) (status, error) {
	stat, err := blizzard.NewStatusFromFilepath(relativeFilepath)
	if err != nil {
		return status{}, err
	}

	return newStatus(reg, stat), nil
}

func newStatus(reg Region, stat blizzard.Status) status {
	return status{stat, reg, newRealms(reg, stat.Realms)}
}

type status struct {
	blizzard.Status
	region Region
	Realms Realms `json:"Realms"`
}

type Statuses map[RegionName]status
