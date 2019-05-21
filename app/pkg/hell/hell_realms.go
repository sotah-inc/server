package hell

import (
	"fmt"

	"cloud.google.com/go/firestore"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/hell/collections"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/util"
)

func (c Client) GetRealm(realmRef *firestore.DocumentRef) (Realm, error) {
	docsnap, err := realmRef.Get(c.Context)
	if err != nil {
		return Realm{}, err
	}

	var realmData Realm
	if err := docsnap.DataTo(&realmData); err != nil {
		return Realm{}, err
	}

	return realmData, nil
}

type WriteRegionRealmsJob struct {
	Err        error
	RegionName blizzard.RegionName
	RealmSlug  blizzard.RealmSlug
	Realm      Realm
}

func (c Client) WriteRegionRealms(regionRealms RegionRealmsMap, version gameversions.GameVersion) error {
	// spawning workers
	in := make(chan WriteRegionRealmsJob)
	out := make(chan WriteRegionRealmsJob)
	worker := func() {
		for inJob := range in {
			realmRef, err := c.FirmDocument(fmt.Sprintf(
				"%s/%s/%s/%s/%s/%s",
				collections.Games,
				version,
				collections.Regions,
				inJob.RegionName,
				collections.Realms,
				inJob.RealmSlug,
			))
			if err != nil {
				inJob.Err = err
				out <- inJob

				continue
			}

			if _, err := realmRef.Set(c.Context, inJob.Realm); err != nil {
				inJob.Err = err
				out <- inJob

				continue
			}

			out <- inJob
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// spinning it up
	go func() {
		for regionName, realms := range regionRealms {
			for realmSlug, realm := range realms {
				in <- WriteRegionRealmsJob{
					RegionName: regionName,
					RealmSlug:  realmSlug,
					Realm:      realm,
				}
			}
		}

		close(in)
	}()

	// waiting for results to drain out
	for job := range out {
		if job.Err != nil {
			return job.Err
		}
	}

	return nil
}

type GetRegionRealmsJob struct {
	Err        error
	RegionName blizzard.RegionName
	RealmSlug  blizzard.RealmSlug
	Realm      Realm
}

func (c Client) GetRegionRealms(
	regionRealmSlugs map[blizzard.RegionName][]blizzard.RealmSlug,
	version gameversions.GameVersion,
) (RegionRealmsMap, error) {
	// spawning workers
	in := make(chan GetRegionRealmsJob)
	out := make(chan GetRegionRealmsJob)
	worker := func() {
		for inJob := range in {
			realmRef, err := c.FirmDocument(fmt.Sprintf(
				"%s/%s/%s/%s/%s/%s",
				collections.Games,
				version,
				collections.Regions,
				inJob.RegionName,
				collections.Realms,
				inJob.RealmSlug,
			))
			if err != nil {
				inJob.Err = err
				out <- inJob

				continue
			}

			realm, err := c.GetRealm(realmRef)
			if err != nil {
				inJob.Err = err
				out <- inJob

				continue
			}

			inJob.Realm = realm
			out <- inJob
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// spinning it up
	go func() {
		for regionName, realmSlugs := range regionRealmSlugs {
			for _, realmSlug := range realmSlugs {
				in <- GetRegionRealmsJob{
					RegionName: regionName,
					RealmSlug:  realmSlug,
				}
			}
		}

		close(in)
	}()

	// waiting for results to drain out
	regionRealms := RegionRealmsMap{}
	for job := range out {
		if job.Err != nil {
			return RegionRealmsMap{}, job.Err
		}

		realms := func() RealmsMap {
			foundRealms, ok := regionRealms[job.RegionName]
			if !ok {
				return RealmsMap{}
			}

			return foundRealms
		}()
		realms[job.RealmSlug] = job.Realm
		regionRealms[job.RegionName] = realms
	}

	return regionRealms, nil
}

type Realm struct {
	Downloaded                 int `firestore:"downloaded"`
	LiveAuctionsReceived       int `firestore:"live_auctions_received"`
	PricelistHistoriesReceived int `firestore:"pricelist_histories_received"`
}

func (r Realm) ToRealmModificationDates() sotah.RealmModificationDates {
	return sotah.RealmModificationDates{
		Downloaded:                 int64(r.Downloaded),
		LiveAuctionsReceived:       int64(r.LiveAuctionsReceived),
		PricelistHistoriesReceived: int64(r.PricelistHistoriesReceived),
	}
}

type RealmsMap map[blizzard.RealmSlug]Realm

type RegionRealmsMap map[blizzard.RegionName]RealmsMap

func (m RegionRealmsMap) ToRegionRealmModificationDates() sotah.RegionRealmModificationDates {
	out := sotah.RegionRealmModificationDates{}
	for regionName, realms := range m {
		out[regionName] = map[blizzard.RealmSlug]sotah.RealmModificationDates{}
		for realmSlug, realm := range realms {
			out[regionName][realmSlug] = realm.ToRealmModificationDates()
		}
	}

	return out
}

func (m RegionRealmsMap) Total() int {
	out := 0

	for _, realms := range m {
		out += len(realms)
	}

	return out
}

func (m RegionRealmsMap) Merge(in RegionRealmsMap) RegionRealmsMap {
	logging.WithFields(logrus.Fields{
		"current":  m.Total(),
		"provided": in.Total(),
	}).Info("Merging in")

	for regionName, hellRealms := range in {
		nextHellRealms := func() RealmsMap {
			foundHellRealms, ok := m[regionName]
			if !ok {
				return RealmsMap{}
			}

			return foundHellRealms
		}()

		for realmSlug, hellRealm := range hellRealms {
			nextHellRealms[realmSlug] = hellRealm
		}

		m[regionName] = nextHellRealms
	}

	return m
}
