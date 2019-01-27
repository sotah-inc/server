package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/sotah-inc/server/app/pkg/util"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
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

func (sto Store) getRealmAuctionsObjectAtTime(bkt *storage.BucketHandle, targetTime time.Time) (*storage.ObjectHandle, error) {
	logging.WithField("targetTime", targetTime.Unix()).Debug("Fetching realm-auctions object at time")

	exists, err := sto.realmAuctionsObjectExists(bkt, targetTime)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, errors.New("realm auctions object does not exist")
	}

	return sto.getRealmAuctionsObject(bkt, targetTime), nil
}

func (sto Store) WriteRealmAuctions(rea sotah.Realm, lastModified time.Time, gzipEncodedBody []byte) error {
	bkt, err := sto.resolveRealmAuctionsBucket(rea)
	if err != nil {
		return err
	}

	logging.WithFields(logrus.Fields{
		"region": rea.Region.Name,
		"realm":  rea.Slug,
		"length": len(gzipEncodedBody),
	}).Debug("Writing auctions to gcloud storage")

	wc := bkt.Object(sto.GetRealmAuctionsObjectName(lastModified)).NewWriter(sto.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"

	if _, err := wc.Write(gzipEncodedBody); err != nil {
		return err
	}

	return wc.Close()
}

type LoadAuctionsInJob struct {
	Realm      sotah.Realm
	TargetTime time.Time
	Auctions   blizzard.Auctions
}

type LoadAuctionsOutJob struct {
	Err        error
	Realm      sotah.Realm
	TargetTime time.Time
	ItemIds    []blizzard.ItemID
}

func (job LoadAuctionsOutJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":       job.Err.Error(),
		"region":      job.Realm.Region.Name,
		"realm":       job.Realm.Slug,
		"target-time": job.TargetTime.Unix(),
	}
}

func (sto Store) LoadAuctions(in chan LoadAuctionsInJob) chan LoadAuctionsOutJob {
	out := make(chan LoadAuctionsOutJob)

	// spinning up the workers for fetching Auctions
	worker := func() {
		for inJob := range in {
			jsonEncodedData, err := json.Marshal(inJob.Auctions)
			if err != nil {
				out <- LoadAuctionsOutJob{
					Err:        err,
					Realm:      inJob.Realm,
					TargetTime: inJob.TargetTime,
					ItemIds:    []blizzard.ItemID{},
				}

				continue
			}

			gzipEncodedData, err := util.GzipEncode(jsonEncodedData)
			if err != nil {
				out <- LoadAuctionsOutJob{
					Err:        err,
					Realm:      inJob.Realm,
					TargetTime: inJob.TargetTime,
					ItemIds:    []blizzard.ItemID{},
				}

				continue
			}

			if err := sto.WriteRealmAuctions(inJob.Realm, inJob.TargetTime, gzipEncodedData); err != nil {
				out <- LoadAuctionsOutJob{
					Err:        err,
					Realm:      inJob.Realm,
					TargetTime: inJob.TargetTime,
					ItemIds:    []blizzard.ItemID{},
				}

				continue
			}

			outItemIds := []blizzard.ItemID{}
			for _, auc := range inJob.Auctions.Auctions {
				outItemIds = append(outItemIds, auc.Item)
			}

			out <- LoadAuctionsOutJob{
				Err:        nil,
				Realm:      inJob.Realm,
				TargetTime: inJob.TargetTime,
				ItemIds:    outItemIds,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	return out
}

type GetAuctionsFromTimesInJob struct {
	Realm      sotah.Realm
	TargetTime time.Time
}

type RealmTimes map[blizzard.RealmSlug]GetAuctionsFromTimesInJob

func RealmTimesToRealms(rt RealmTimes) sotah.Realms {
	out := sotah.Realms{}
	for _, job := range rt {
		out = append(out, job.Realm)
	}

	return out
}

type RegionRealmTimes map[blizzard.RegionName]RealmTimes

type GetAuctionsFromTimesOutJob struct {
	Err        error
	Realm      sotah.Realm
	TargetTime time.Time
	Auctions   blizzard.Auctions
}

func (job GetAuctionsFromTimesOutJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":       job.Err.Error(),
		"realm":       job.Realm.Slug,
		"target_time": job.TargetTime.Unix(),
	}
}

func (sto Store) GetAuctionsFromTimes(times RealmTimes) chan GetAuctionsFromTimesOutJob {
	in := make(chan GetAuctionsFromTimesInJob)
	out := make(chan GetAuctionsFromTimesOutJob)

	// spinning up the workers for fetching Auctions
	worker := func() {
		for inJob := range in {
			aucs, err := sto.getAuctions(inJob.Realm, inJob.TargetTime)
			if err != nil {
				out <- GetAuctionsFromTimesOutJob{
					Err:        err,
					Realm:      inJob.Realm,
					TargetTime: inJob.TargetTime,
					Auctions:   blizzard.Auctions{},
				}

				continue
			}

			out <- GetAuctionsFromTimesOutJob{err, inJob.Realm, inJob.TargetTime, aucs}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the Realms
	go func() {
		for _, inJob := range times {
			logging.WithFields(logrus.Fields{
				"Region": inJob.Realm.Region.Name,
				"Realm":  inJob.Realm.Slug,
			}).Debug("Queueing up auction for loading")
			in <- inJob
		}

		close(in)
	}()

	return out
}

func (sto Store) getAuctions(rea sotah.Realm, targetTime time.Time) (blizzard.Auctions, error) {
	hasBucket, err := sto.RealmAuctionsBucketExists(rea)
	if err != nil {
		return blizzard.Auctions{}, err
	}

	if !hasBucket {
		logging.WithFields(logrus.Fields{
			"region": rea.Region.Name,
			"realm":  rea.Slug,
		}).Error("Realm has no bucket")

		return blizzard.Auctions{}, errors.New("realm has no bucket")
	}

	bkt := sto.GetRealmAuctionsBucket(rea)
	if err != nil {
		return blizzard.Auctions{}, err
	}

	obj, err := sto.getRealmAuctionsObjectAtTime(bkt, targetTime)
	if err != nil {
		return blizzard.Auctions{}, err
	}

	if obj == nil {
		logging.WithFields(logrus.Fields{
			"region": rea.Region.Name,
			"realm":  rea.Slug,
		}).Error("Found no auctions in Store")

		return blizzard.Auctions{}, errors.New("found no auctions in store at specified time")
	}

	logging.WithFields(logrus.Fields{
		"region": rea.Region.Name,
		"realm":  rea.Slug,
	}).Debug("Loading auctions from Store")

	return sto.NewAuctions(obj)
}

func (sto Store) NewAuctions(obj *storage.ObjectHandle) (blizzard.Auctions, error) {
	reader, err := obj.NewReader(sto.Context)
	if err != nil {
		return blizzard.Auctions{}, err
	}
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		return blizzard.Auctions{}, err
	}

	return blizzard.NewAuctions(body)
}
