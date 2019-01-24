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

func NewRealms(reg Region, blizzRealms []blizzard.Realm) Realms {
	reas := make([]Realm, len(blizzRealms))
	for i, rea := range blizzRealms {
		reas[i] = Realm{rea, reg, 0}
	}

	return reas
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

func (rea Realm) AuctionsFilepath(parentDirPath string) (string, error) {
	return filepath.Abs(
		fmt.Sprintf("%s/Auctions/%s/%s.json.gz", c.CacheDir, rea.Region.Name, rea.Slug),
	)
}

func (rea Realm) loadAuctionsFromFilecache(c *Config) (blizzard.Auctions, time.Time, error) {
	// resolving the cached Auctions filepath
	cachedAuctionsFilepath, err := rea.AuctionsFilepath(c)
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

func NewStatusFromMessenger(reg Region, mess messenger.Messenger) (Status, error) {
	lm := state.StatusRequest{RegionName: reg.Name}
	encodedMessage, err := json.Marshal(lm)
	if err != nil {
		return Status{}, err
	}

	msg, err := mess.Request(subjects.Status, encodedMessage)
	if err != nil {
		return Status{}, err
	}

	if msg.Code != codes.Ok {
		return Status{}, errors.New(msg.Err)
	}

	stat, err := blizzard.NewStatus([]byte(msg.Data))
	if err != nil {
		return Status{}, err
	}

	return newStatus(reg, stat), nil
}

func NewStatusFromFilepath(reg Region, relativeFilepath string) (Status, error) {
	stat, err := blizzard.NewStatusFromFilepath(relativeFilepath)
	if err != nil {
		return Status{}, err
	}

	return newStatus(reg, stat), nil
}

func newStatus(reg Region, stat blizzard.Status) Status {
	return Status{stat, reg, NewRealms(reg, stat.Realms)}
}

type Status struct {
	blizzard.Status
	Region Region
	Realms Realms `json:"Realms"`
}

type Statuses map[RegionName]Status
