package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	storage "cloud.google.com/go/storage"
	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/util"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

func (sto store) getRealmAuctionsBucketName(rea realm) string {
	return fmt.Sprintf("raw-auctions_%s_%s", rea.region.Name, rea.Slug)
}

func (sto store) getRealmAuctionsBucket(rea realm) *storage.BucketHandle {
	return sto.client.Bucket(sto.getRealmAuctionsBucketName(rea))
}

func (sto store) createRealmAuctionsBucket(rea realm) (*storage.BucketHandle, error) {
	bkt := sto.getRealmAuctionsBucket(rea)
	err := bkt.Create(sto.context, sto.projectID, &storage.BucketAttrs{
		StorageClass: "REGIONAL",
		Location:     "us-east1",
	})
	if err != nil {
		return nil, err
	}

	return bkt, nil
}

func (sto store) realmAuctionsBucketExists(rea realm) (bool, error) {
	_, err := sto.getRealmAuctionsBucket(rea).Attrs(sto.context)
	if err != nil {
		if err != storage.ErrBucketNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (sto store) resolveRealmAuctionsBucket(rea realm) (*storage.BucketHandle, error) {
	exists, err := sto.realmAuctionsBucketExists(rea)
	if err != nil {
		return nil, err
	}

	if !exists {
		return sto.createRealmAuctionsBucket(rea)
	}

	return sto.getRealmAuctionsBucket(rea), nil
}

func (sto store) getRealmAuctionsObjectName(lastModified time.Time) string {
	return fmt.Sprintf("%d.json.gz", lastModified.Unix())
}

func (sto store) getRealmAuctionsObject(bkt *storage.BucketHandle, lastModified time.Time) *storage.ObjectHandle {
	return bkt.Object(sto.getRealmAuctionsObjectName(lastModified))
}

func (sto store) realmAuctionsObjectExists(bkt *storage.BucketHandle, lastModified time.Time) (bool, error) {
	_, err := sto.getRealmAuctionsObject(bkt, lastModified).Attrs(sto.context)
	if err != nil {
		if err != storage.ErrObjectNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (sto store) writeRealmAuctions(rea realm, lastModified time.Time, body []byte) error {
	bkt, err := sto.resolveRealmAuctionsBucket(rea)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"region": rea.region.Name,
		"realm":  rea.Slug,
		"length": len(body),
	}).Debug("Writing auctions to gcloud storage")

	wc := bkt.Object(sto.getRealmAuctionsObjectName(lastModified)).NewWriter(sto.context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	wc.Write(body)
	return wc.Close()
}

func (sto store) getTotalRealmAuctionsSize(rea realm) (int64, error) {
	log.WithFields(log.Fields{
		"region": rea.region.Name,
		"realm":  rea.Slug,
	}).Debug("Gathering total bucket size")

	exists, err := sto.realmAuctionsBucketExists(rea)
	if err != nil {
		return 0, err
	}

	if !exists {
		return 0, nil
	}

	it := sto.getRealmAuctionsBucket(rea).Objects(sto.context, nil)
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

type getTotalRealmAuctionSizeJob struct {
	realm     realm
	totalSize int64
	err       error
}

func (sto store) getTotalRealmsAuctionSize(reas realms) chan getTotalRealmAuctionSizeJob {
	// establishing channels
	out := make(chan getTotalRealmAuctionSizeJob)
	in := make(chan realm)

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

func (sto store) loadRegionRealmMap(rMap realmMap) chan loadAuctionsJob {
	// establishing channels
	out := make(chan loadAuctionsJob)
	in := make(chan realmMapValue)

	// spinning up the workers for fetching auctions
	worker := func() {
		for rValue := range in {
			aucs, lastModified, err := sto.loadRealmAuctions(rValue.realm, rValue.lastModified)
			if err != nil {
				log.WithFields(log.Fields{
					"region": rValue.realm.region.Name,
					"realm":  rValue.realm.Slug,
					"error":  err.Error(),
				}).Info("Failed to load store auctions")

				out <- loadAuctionsJob{err, rValue.realm, blizzard.Auctions{}, time.Time{}}

				continue
			}

			if lastModified.IsZero() {
				log.WithFields(log.Fields{
					"region": rValue.realm.region.Name,
					"realm":  rValue.realm.Slug,
				}).Info("No auctions were loaded")

				continue
			}

			out <- loadAuctionsJob{err, rValue.realm, aucs, lastModified}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the realms
	go func() {
		for _, rValue := range rMap.values {
			log.WithField("realm", rValue.realm.Slug).Debug("Queueing up auction for store loading")
			in <- rValue
		}

		close(in)
	}()

	return out
}

func (sto store) loadRealmsAuctions(c *config, reas realms) chan loadAuctionsJob {
	// establishing channels
	out := make(chan loadAuctionsJob)
	in := make(chan realm)

	// spinning up the workers for fetching auctions
	worker := func() {
		for rea := range in {
			aucs, lastModified, err := sto.loadRealmAuctions(rea, time.Time{})
			if err != nil {
				log.WithFields(log.Fields{
					"region": rea.region.Name,
					"realm":  rea.Slug,
					"error":  err.Error(),
				}).Info("Failed to load store auctions")

				out <- loadAuctionsJob{err, rea, blizzard.Auctions{}, time.Time{}}

				continue
			}

			if lastModified.IsZero() {
				log.WithFields(log.Fields{
					"region": rea.region.Name,
					"realm":  rea.Slug,
				}).Info("No auctions were loaded")

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

			log.WithField("realm", rea.Slug).Debug("Queueing up auction for store loading")
			in <- rea
		}

		close(in)
	}()

	return out
}

func (sto store) getRealmAuctionsObjectAtTimeOrLatest(bkt *storage.BucketHandle, targetTime time.Time) (*storage.ObjectHandle, time.Time, error) {
	if targetTime.IsZero() {
		return sto.getLatestRealmAuctionsObject(bkt)
	}

	obj, err := sto.getRealmAuctionsObjectAtTime(bkt, targetTime)
	if err != nil {
		return nil, time.Time{}, err
	}

	return obj, targetTime, nil
}

func (sto store) getRealmAuctionsObjectAtTime(bkt *storage.BucketHandle, targetTime time.Time) (*storage.ObjectHandle, error) {
	log.WithField("target-time", targetTime.Unix()).Info("Fetching realm-auctions object at time")

	exists, err := sto.realmAuctionsObjectExists(bkt, targetTime)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, errors.New("Realm auctions object does not exist")
	}

	return sto.getRealmAuctionsObject(bkt, targetTime), nil
}

func (sto store) getLatestRealmAuctionsObject(bkt *storage.BucketHandle) (*storage.ObjectHandle, time.Time, error) {
	log.Info("Fetching latest realm-auctions object")

	var obj *storage.ObjectHandle
	var objAttrs *storage.ObjectAttrs
	lastCreated := time.Time{}
	it := bkt.Objects(sto.context, nil)
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

func (sto store) loadRealmAuctions(rea realm, targetTime time.Time) (blizzard.Auctions, time.Time, error) {
	hasBucket, err := sto.realmAuctionsBucketExists(rea)
	if err != nil {
		return blizzard.Auctions{}, time.Time{}, err
	}

	if !hasBucket {
		log.WithFields(log.Fields{
			"region": rea.region.Name,
			"realm":  rea.Slug,
		}).Info("Realm has no bucket")

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
		rea.LogEntry().Info("Found no auctions in store")

		return blizzard.Auctions{}, time.Time{}, nil
	}

	rea.LogEntry().Info("Loading auctions from store")

	aucs, err := blizzard.NewAuctionsFromGcloudObject(sto.context, obj)
	if err != nil {
		log.WithFields(log.Fields{
			"region": rea.region.Name,
			"realm":  rea.Slug,
		}).Info("Failed to parse realm auctions, deleting")

		if err := obj.Delete(sto.context); err != nil {
			return blizzard.Auctions{}, time.Time{}, err
		}

		return blizzard.Auctions{}, time.Time{}, nil
	}

	log.WithFields(log.Fields{
		"region": rea.region.Name,
		"realm":  rea.Slug,
	}).Info("Loaded auctions from store")

	return aucs, lastModified, nil
}
