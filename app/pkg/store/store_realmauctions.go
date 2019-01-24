package store

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sotah-inc/server/app/pkg/sotah"

	storage "cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/store/objstate"
	"github.com/sotah-inc/server/app/pkg/util"
	"google.golang.org/api/iterator"
)

func (sto Store) getRealmAuctionsBucketName(rea sotah.Realm) string {
	return fmt.Sprintf("raw-auctions_%s_%s", rea.Region.Name, rea.Slug)
}

func (sto Store) GetRealmAuctionsBucket(rea sotah.Realm) *storage.BucketHandle {
	return sto.client.Bucket(sto.getRealmAuctionsBucketName(rea))
}

func (sto Store) createRealmAuctionsBucket(rea sotah.Realm) (*storage.BucketHandle, error) {
	bkt := sto.GetRealmAuctionsBucket(rea)
	err := bkt.Create(sto.Context, sto.projectID, &storage.BucketAttrs{
		StorageClass: "REGIONAL",
		Location:     "us-east1",
	})
	if err != nil {
		return nil, err
	}

	return bkt, nil
}

func (sto Store) RealmAuctionsBucketExists(rea sotah.Realm) (bool, error) {
	_, err := sto.GetRealmAuctionsBucket(rea).Attrs(sto.Context)
	if err != nil {
		if err != storage.ErrBucketNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (sto Store) resolveRealmAuctionsBucket(rea sotah.Realm) (*storage.BucketHandle, error) {
	exists, err := sto.RealmAuctionsBucketExists(rea)
	if err != nil {
		return nil, err
	}

	if !exists {
		return sto.createRealmAuctionsBucket(rea)
	}

	return sto.GetRealmAuctionsBucket(rea), nil
}

func (sto Store) GetRealmAuctionsObjectName(lastModified time.Time) string {
	return fmt.Sprintf("%d.json.gz", lastModified.Unix())
}

func (sto Store) getRealmAuctionsObject(bkt *storage.BucketHandle, lastModified time.Time) *storage.ObjectHandle {
	return bkt.Object(sto.GetRealmAuctionsObjectName(lastModified))
}

func (sto Store) realmAuctionsObjectExists(bkt *storage.BucketHandle, lastModified time.Time) (bool, error) {
	_, err := sto.getRealmAuctionsObject(bkt, lastModified).Attrs(sto.Context)
	if err != nil {
		if err != storage.ErrObjectNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (sto Store) WriteRealmAuctions(rea sotah.Realm, lastModified time.Time, body []byte) error {
	bkt, err := sto.resolveRealmAuctionsBucket(rea)
	if err != nil {
		return err
	}

	logging.WithFields(logrus.Fields{
		"region": rea.Region.Name,
		"realm":  rea.Slug,
		"length": len(body),
	}).Debug("Writing auctions to gcloud storage")

	wc := bkt.Object(sto.GetRealmAuctionsObjectName(lastModified)).NewWriter(sto.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"

	if _, err := wc.Write(body); err != nil {
		return err
	}

	return wc.Close()
}

func (sto Store) getTotalRealmAuctionsSize(rea sotah.Realm) (int64, error) {
	logging.WithFields(logrus.Fields{
		"region": rea.Region.Name,
		"realm":  rea.Slug,
	}).Debug("Gathering total bucket size")

	exists, err := sto.RealmAuctionsBucketExists(rea)
	if err != nil {
		return 0, err
	}

	if !exists {
		return 0, nil
	}

	it := sto.GetRealmAuctionsBucket(rea).Objects(sto.Context, nil)
	totalSize := int64(0)
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}

			return 0, err
		}

		totalSize += objAttrs.Size
	}

	return totalSize, nil
}

type storeCollectJob struct {
	obj        *storage.ObjectHandle
	realm      sotah.Realm
	targetTime time.Time
}

func (sto Store) collectRegionRealms(c internal.Config, regs internal.RegionList, stas internal.Statuses) chan storeCollectJob {
	// establishing channels
	out := make(chan storeCollectJob)
	in := make(chan sotah.Realm)

	// spinning up the workers for gathering total realm auction size
	worker := func() {
		for rea := range in {
			// validating that the realm-auctions bucket exists
			exists, err := sto.RealmAuctionsBucketExists(rea)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": rea.Region.Name,
					"realm":  rea.Slug,
				}).Error("Failed to check if realm-auctions bucket exists")

				continue
			}
			if exists == false {
				continue
			}

			logging.WithFields(logrus.Fields{
				"region": rea.Region.Name,
				"realm":  rea.Slug,
			}).Debug("Checking Store for realm-auctions-object for processing")

			// checking the Store for the latest realm-auctions object for processing
			bkt := sto.GetRealmAuctionsBucket(rea)
			obj, targetTime, err := sto.getLatestRealmAuctionsObjectForProcessing(bkt)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": rea.Region.Name,
					"realm":  rea.Slug,
				}).Error("Failed to fetch latest realm-auctions object for processing")

				continue
			}

			// optionally halting on no results returned
			if targetTime.IsZero() {
				logging.WithFields(logrus.Fields{
					"region": rea.Region.Name,
					"realm":  rea.Slug,
				}).Debug("No results found for processing via auctions-intake collector")

				continue
			}

			out <- storeCollectJob{obj, rea, targetTime}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the realms
	go func() {
		for _, reg := range regs {
			for _, rea := range stas[reg.Name].Realms {
				in <- rea
			}
		}

		close(in)
	}()

	return out
}

func (sto Store) startCollector(c internal.Config, regs []internal.Region, stas internal.Statuses, collectOut chan state.AuctionsIntakeRequest) {
	logging.Info("Starting auctions-intake collector")

	for {
		if true {
			break
		}

		aiRequest := state.AuctionsIntakeRequest{RegionRealmTimestamps: state.IntakeRequestData{}}
		for _, reg := range regs {
			aiRequest.RegionRealmTimestamps[reg.Name] = map[blizzard.RealmSlug]int64{}
		}

		hasResults := false
		scJobs := sto.collectRegionRealms(c, regs, stas)
		for job := range scJobs {
			// gathering obj attrs for updating metadata
			objAttrs, err := job.obj.Attrs(sto.Context)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.realm.Region.Name,
					"realm":  job.realm.Slug,
				}).Error("Failed to gathering obj attrs")

				continue
			}

			hasResults = true
			aiRequest.RegionRealmTimestamps[job.realm.Region.Name][job.realm.Slug] = job.targetTime.Unix()

			objMeta := func() map[string]string {
				if objAttrs.Metadata == nil {
					return map[string]string{}
				}

				return objAttrs.Metadata
			}()
			objMeta["state"] = fmt.Sprintf(string(objstate.Queued), ID.String())
			if _, err := job.obj.Update(sto.Context, storage.ObjectAttrsToUpdate{Metadata: objMeta}); err != nil {
				logging.WithFields(logrus.Fields{
					"error":         err.Error(),
					"region":        job.realm.Region.Name,
					"realm":         job.realm.Slug,
					"last-modified": job.targetTime.Unix(),
				}).Error("Failed to update metadata of object")

				continue
			}
		}

		if hasResults == false {
			logging.Info("Breaking due to no realm-auctions results found")

			break
		}

		logging.WithField("buffer-size", len(collectOut)).Info("Queueing auctions-intake-request from the collector")
		collectOut <- aiRequest
	}
}

type getTotalRealmAuctionSizeJob struct {
	realm     internal.Realm
	totalSize int64
	err       error
}

func (sto Store) getTotalRealmsAuctionSize(reas internal.Realms) chan getTotalRealmAuctionSizeJob {
	// establishing channels
	out := make(chan getTotalRealmAuctionSizeJob)
	in := make(chan internal.Realm)

	// spinning up the workers for gathering total realm auction size
	worker := func() {
		for rea := range in {
			totalSize, err := sto.getTotalRealmAuctionsSize(rea)
			out <- getTotalRealmAuctionSizeJob{rea, totalSize, err}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the realms
	go func() {
		for _, rea := range reas {
			in <- rea
		}

		close(in)
	}()

	return out
}

func (sto Store) LoadRegionRealmMap(rMap state.RealmMap) chan internal.LoadAuctionsJob {
	// establishing channels
	out := make(chan internal.LoadAuctionsJob)
	in := make(chan state.RealmMapValue)

	// spinning up the workers for fetching auctions
	worker := func() {
		for rValue := range in {
			aucs, lastModified, err := sto.loadRealmAuctions(rValue.Realm, rValue.LastModified)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": rValue.Realm.Region.Name,
					"realm":  rValue.Realm.Slug,
				}).Error("Failed to load Store auctions")

				out <- internal.LoadAuctionsJob{
					Err:          err,
					Realm:        rValue.Realm,
					Auctions:     blizzard.Auctions{},
					LastModified: time.Time{},
				}

				continue
			}

			if lastModified.IsZero() {
				logging.WithFields(logrus.Fields{
					"region": rValue.Realm.Region.Name,
					"realm":  rValue.Realm.Slug,
				}).Info("No auctions were loaded")

				continue
			}

			out <- internal.LoadAuctionsJob{
				Err:          err,
				Realm:        rValue.Realm,
				Auctions:     aucs,
				LastModified: lastModified,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the realms
	go func() {
		for _, rValue := range rMap.Values {
			logging.WithField("realm", rValue.Realm.Slug).Debug("Queueing up auction for Store loading")
			in <- rValue
		}

		close(in)
	}()

	return out
}

func (sto Store) LoadRealmsAuctions(c *internal.Config, reas internal.Realms) chan internal.LoadAuctionsJob {
	// establishing channels
	out := make(chan internal.LoadAuctionsJob)
	in := make(chan internal.Realm)

	// spinning up the workers for fetching auctions
	worker := func() {
		for rea := range in {
			aucs, lastModified, err := sto.loadRealmAuctions(rea, time.Time{})
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": rea.Region.Name,
					"realm":  rea.Slug,
				}).Error("Failed to load Store auctions")

				out <- internal.LoadAuctionsJob{
					Err:          err,
					Realm:        rea,
					Auctions:     blizzard.Auctions{},
					LastModified: time.Time{},
				}

				continue
			}

			if lastModified.IsZero() {
				logging.WithFields(logrus.Fields{
					"region": rea.Region.Name,
					"realm":  rea.Slug,
				}).Info("No auctions were loaded")

				continue
			}

			out <- internal.LoadAuctionsJob{
				Err:          err,
				Realm:        rea,
				Auctions:     aucs,
				LastModified: lastModified,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the realms
	go func() {
		for _, rea := range reas {
			wList := c.GetRegionWhitelist(rea.Region.Name)
			if wList != nil {
				resolvedWhiteList := *wList
				if _, ok := resolvedWhiteList[rea.Slug]; !ok {
					continue
				}
			}

			logging.WithField("realm", rea.Slug).Debug("Queueing up auction for Store loading")
			in <- rea
		}

		close(in)
	}()

	return out
}

func (sto Store) getRealmAuctionsObjectAtTimeOrLatest(bkt *storage.BucketHandle, targetTime time.Time) (*storage.ObjectHandle, time.Time, error) {
	if targetTime.IsZero() {
		return sto.getLatestRealmAuctionsObject(bkt)
	}

	obj, err := sto.getRealmAuctionsObjectAtTime(bkt, targetTime)
	if err != nil {
		return nil, time.Time{}, err
	}

	return obj, targetTime, nil
}

func (sto Store) getRealmAuctionsObjectAtTime(bkt *storage.BucketHandle, targetTime time.Time) (*storage.ObjectHandle, error) {
	logging.WithField("targetTime", targetTime.Unix()).Debug("Fetching realm-auctions object at time")

	exists, err := sto.realmAuctionsObjectExists(bkt, targetTime)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, errors.New("Realm auctions object does not exist")
	}

	return sto.getRealmAuctionsObject(bkt, targetTime), nil
}

func (sto Store) getLatestRealmAuctionsObject(bkt *storage.BucketHandle) (*storage.ObjectHandle, time.Time, error) {
	logging.Debug("Fetching latest realm-auctions object from bucket")

	var obj *storage.ObjectHandle
	var objAttrs *storage.ObjectAttrs
	lastCreated := time.Time{}
	it := bkt.Objects(sto.Context, nil)
	for {
		nextObjAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, time.Time{}, err
		}

		if obj == nil || lastCreated.IsZero() || lastCreated.Before(nextObjAttrs.Created) {
			obj = bkt.Object(nextObjAttrs.Name)
			objAttrs = nextObjAttrs
			lastCreated = nextObjAttrs.Created
		}
	}

	if obj == nil {
		return nil, time.Time{}, nil
	}

	s := strings.Split(objAttrs.Name, ".")
	lastModifiedUnix, err := strconv.Atoi(s[0])
	if err != nil {
		return nil, time.Time{}, err
	}
	lastModified := time.Unix(int64(lastModifiedUnix), 0)

	return obj, lastModified, nil
}

func (sto Store) getLatestRealmAuctionsObjectForProcessing(bkt *storage.BucketHandle) (*storage.ObjectHandle, time.Time, error) {
	var obj *storage.ObjectHandle
	var objAttrs *storage.ObjectAttrs
	lastCreated := time.Time{}
	it := bkt.Objects(sto.Context, nil)
	earliestTime := time.Now().Add(-1 * time.Hour * 24 * 14)
	for {
		nextObjAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, time.Time{}, err
		}

		if nextObjAttrs.Created.Before(earliestTime) {
			continue
		}

		metaState := func() string {
			if nextObjAttrs.Metadata == nil {
				return ""
			}

			state, ok := nextObjAttrs.Metadata["state"]
			if !ok {
				return ""
			}

			return state
		}()

		if metaState == string(objstate.Processed) || metaState == fmt.Sprintf(string(objstate.Queued), ID.String()) {
			continue
		}

		if obj == nil || lastCreated.IsZero() || lastCreated.Before(nextObjAttrs.Created) {
			obj = bkt.Object(nextObjAttrs.Name)
			objAttrs = nextObjAttrs
			lastCreated = nextObjAttrs.Created
		}
	}

	if obj == nil {
		return nil, time.Time{}, nil
	}

	s := strings.Split(objAttrs.Name, ".")
	lastModifiedUnix, err := strconv.Atoi(s[0])
	if err != nil {
		return nil, time.Time{}, err
	}
	lastModified := time.Unix(int64(lastModifiedUnix), 0)

	return obj, lastModified, nil
}

func (sto Store) loadRealmAuctions(rea internal.Realm, targetTime time.Time) (blizzard.Auctions, time.Time, error) {
	hasBucket, err := sto.RealmAuctionsBucketExists(rea)
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}

	if !hasBucket {
		logging.WithFields(logrus.Fields{
			"region": rea.Region.Name,
			"realm":  rea.Slug,
		}).Error("Realm has no bucket")

		return blizzard.Auctions{}, time.Time{}, nil
	}

	bkt, err := sto.resolveRealmAuctionsBucket(rea)
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}

	obj, lastModified, err := sto.getRealmAuctionsObjectAtTimeOrLatest(bkt, targetTime)
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}

	if obj == nil {
		logging.WithFields(logrus.Fields{
			"region": rea.Region.Name,
			"realm":  rea.Slug,
		}).Info("Found no auctions in Store")

		return blizzard.Auctions{}, time.Time{}, nil
	}

	logging.WithFields(logrus.Fields{
		"region": rea.Region.Name,
		"realm":  rea.Slug,
	}).Debug("Loading auctions from Store")

	aucs, err := blizzard.NewAuctionsFromGcloudObject(sto.Context, obj)
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":  err.Error(),
			"region": rea.Region.Name,
			"realm":  rea.Slug,
		}).Error("Failed to parse realm auctions, deleting and returning blank auctions")

		if err := obj.Delete(sto.Context); err != nil {
			return blizzard.Auctions{}, time.Time{}, err
		}

		return blizzard.Auctions{}, time.Time{}, nil
	}

	logging.WithFields(logrus.Fields{
		"region": rea.Region.Name,
		"realm":  rea.Slug,
	}).Debug("Loaded auctions from Store")

	return aucs, lastModified, nil
}
