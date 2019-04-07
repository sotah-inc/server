package store

import (
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/store/regions"
	"github.com/sotah-inc/server/app/pkg/util"
	"github.com/twinj/uuid"
	"google.golang.org/api/iterator"
)

func NewPricelistHistoriesBaseV2(c Client, location regions.Region) PricelistHistoriesBaseV2 {
	return PricelistHistoriesBaseV2{base{client: c, location: location}}
}

type PricelistHistoriesBaseV2 struct {
	base
}

func (b PricelistHistoriesBaseV2) getBucketName() string {
	return "pricelist-histories"
}

func (b PricelistHistoriesBaseV2) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b PricelistHistoriesBaseV2) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b PricelistHistoriesBaseV2) getObjectName(targetTime time.Time, realm sotah.Realm) string {
	return fmt.Sprintf("%s/%s/%d.txt.gz", realm.Region.Name, realm.Slug, targetTime.Unix())
}

func (b PricelistHistoriesBaseV2) GetObject(targetTime time.Time, realm sotah.Realm, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(targetTime, realm), bkt)
}

func (b PricelistHistoriesBaseV2) GetFirmObject(targetTime time.Time, realm sotah.Realm, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(b.getObjectName(targetTime, realm), bkt)
}

func (b PricelistHistoriesBaseV2) Handle(aucs blizzard.Auctions, targetTime time.Time, rea sotah.Realm, bkt *storage.BucketHandle) (sotah.UnixTimestamp, error) {
	normalizedTargetDate := sotah.NormalizeTargetDate(targetTime)

	// resolving unix-timestamp of target-time
	targetTimestamp := sotah.UnixTimestamp(targetTime.Unix())

	// gathering an object
	obj := b.GetObject(normalizedTargetDate, rea, bkt)

	// resolving item-price-histories
	ipHistories, err := func() (sotah.ItemPriceHistories, error) {
		exists, err := b.ObjectExists(obj)
		if err != nil {
			return sotah.ItemPriceHistories{}, err
		}

		if !exists {
			return sotah.ItemPriceHistories{}, nil
		}

		reader, err := obj.NewReader(b.client.Context)
		if err != nil {
			return sotah.ItemPriceHistories{}, err
		}
		defer reader.Close()

		return sotah.NewItemPriceHistoriesFromMinimized(reader)
	}()
	if err != nil {
		return 0, err
	}

	// gathering new item-prices from the input
	iPrices := sotah.NewItemPrices(sotah.NewMiniAuctionListFromMiniAuctions(sotah.NewMiniAuctions(aucs)))

	// merging item-prices into the item-price-histories
	for itemId, prices := range iPrices {
		pHistory := func() sotah.PriceHistory {
			result, ok := ipHistories[itemId]
			if !ok {
				return sotah.PriceHistory{}
			}

			return result
		}()
		pHistory[targetTimestamp] = prices

		ipHistories[itemId] = pHistory
	}

	// encoding the item-price-histories for persistence
	gzipEncodedBody, err := ipHistories.EncodeForPersistence()
	if err != nil {
		return 0, err
	}

	// writing it out to the gcloud object
	wc := obj.NewWriter(b.client.Context)
	wc.ContentType = "text/plain"
	wc.ContentEncoding = "gzip"
	if wc.Metadata == nil {
		wc.Metadata = map[string]string{}
	}
	wc.Metadata["version_id"] = uuid.NewV4().String()
	if _, err := wc.Write(gzipEncodedBody); err != nil {
		return 0, err
	}
	if err := wc.Close(); err != nil {
		return 0, err
	}

	return sotah.UnixTimestamp(normalizedTargetDate.Unix()), wc.Close()
}

type GetAllPricelistHistoriesInJob struct {
	RegionName      blizzard.RegionName
	RealmSlug       blizzard.RealmSlug
	TargetTimestamp sotah.UnixTimestamp
}

type GetAllPricelistHistoriesOutJob struct {
	Err             error
	RegionName      blizzard.RegionName
	RealmSlug       blizzard.RealmSlug
	TargetTimestamp sotah.UnixTimestamp
	Data            map[blizzard.ItemID][]byte
	VersionId       string
}

func (job GetAllPricelistHistoriesOutJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":            job.Err.Error(),
		"region":           job.RegionName,
		"realm":            job.RealmSlug,
		"target-timestamp": job.TargetTimestamp,
	}
}

func (b PricelistHistoriesBaseV2) GetAll(
	in chan GetAllPricelistHistoriesInJob,
	bkt *storage.BucketHandle,
) chan GetAllPricelistHistoriesOutJob {
	out := make(chan GetAllPricelistHistoriesOutJob)

	// spinning up some workers
	worker := func() {
		for inJob := range in {
			// resolving the obj
			obj, err := b.GetFirmObject(
				time.Unix(int64(inJob.TargetTimestamp), 0),
				sotah.NewSkeletonRealm(inJob.RegionName, inJob.RealmSlug),
				bkt,
			)
			if err != nil {
				out <- GetAllPricelistHistoriesOutJob{
					Err:             err,
					RegionName:      inJob.RegionName,
					RealmSlug:       inJob.RealmSlug,
					TargetTimestamp: inJob.TargetTimestamp,
				}

				continue
			}

			objAttrs, err := obj.Attrs(b.client.Context)
			if err != nil {
				out <- GetAllPricelistHistoriesOutJob{
					Err:             err,
					RegionName:      inJob.RegionName,
					RealmSlug:       inJob.RealmSlug,
					TargetTimestamp: inJob.TargetTimestamp,
				}

				continue
			}

			// resolving the data
			data, err := func() (map[blizzard.ItemID][]byte, error) {
				// gathering the data from the object
				reader, err := obj.NewReader(b.client.Context)
				if err != nil {
					return map[blizzard.ItemID][]byte{}, err
				}
				defer reader.Close()

				out := map[blizzard.ItemID][]byte{}
				r := csv.NewReader(reader)
				for {
					record, err := r.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						return map[blizzard.ItemID][]byte{}, err
					}

					itemIdInt, err := strconv.Atoi(record[0])
					if err != nil {
						return map[blizzard.ItemID][]byte{}, err
					}
					itemId := blizzard.ItemID(itemIdInt)

					base64DecodedPriceHistory, err := base64.StdEncoding.DecodeString(record[1])
					if err != nil {
						return map[blizzard.ItemID][]byte{}, err
					}

					out[itemId] = base64DecodedPriceHistory
				}

				return out, nil
			}()
			if err != nil {
				out <- GetAllPricelistHistoriesOutJob{
					Err:             err,
					RegionName:      inJob.RegionName,
					RealmSlug:       inJob.RealmSlug,
					TargetTimestamp: inJob.TargetTimestamp,
				}

				continue
			}

			out <- GetAllPricelistHistoriesOutJob{
				Err:             nil,
				RegionName:      inJob.RegionName,
				RealmSlug:       inJob.RealmSlug,
				TargetTimestamp: inJob.TargetTimestamp,
				Data:            data,
				VersionId:       objAttrs.Metadata["version_id"],
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(2, worker, postWork)

	return out
}

func NewGetAllPricelistHistoriesInJobs(v sotah.PricelistHistoryVersions) []GetAllPricelistHistoriesInJob {
	out := []GetAllPricelistHistoriesInJob{}
	for regionName, realmTimestampVersions := range v {
		for realmSlug, timestampVersions := range realmTimestampVersions {
			for targetTimestamp := range timestampVersions {
				out = append(out, GetAllPricelistHistoriesInJob{
					RegionName:      regionName,
					RealmSlug:       realmSlug,
					TargetTimestamp: targetTimestamp,
				})
			}
		}
	}

	return out
}

type GetVersionsInJob struct {
	RegionName      blizzard.RegionName
	RealmSlug       blizzard.RealmSlug
	TargetTimestamp sotah.UnixTimestamp
}

type GetVersionOutJob struct {
	Err             error
	RegionName      blizzard.RegionName
	RealmSlug       blizzard.RealmSlug
	TargetTimestamp sotah.UnixTimestamp
	Version         string
}

func (b PricelistHistoriesBaseV2) GetVersions(
	regionRealms map[blizzard.RegionName]sotah.Realms,
	bkt *storage.BucketHandle,
) (sotah.PricelistHistoryVersions, error) {
	timestamps, err := b.GetAllTimestamps(regionRealms, bkt)
	if err != nil {
		return sotah.PricelistHistoryVersions{}, err
	}

	inJobs := make(chan GetVersionsInJob)
	outJobs := make(chan GetVersionOutJob)

	// spinning up workers
	worker := func() {
		for inJob := range inJobs {
			realm := sotah.NewSkeletonRealm(inJob.RegionName, inJob.RealmSlug)

			obj, err := b.GetFirmObject(time.Unix(int64(inJob.TargetTimestamp), 0), realm, bkt)
			if err != nil {
				outJobs <- GetVersionOutJob{
					Err:             err,
					RegionName:      inJob.RegionName,
					RealmSlug:       inJob.RealmSlug,
					TargetTimestamp: inJob.TargetTimestamp,
				}

				continue
			}

			objAttrs, err := obj.Attrs(b.client.Context)
			if err != nil {
				outJobs <- GetVersionOutJob{
					Err:             err,
					RegionName:      inJob.RegionName,
					RealmSlug:       inJob.RealmSlug,
					TargetTimestamp: inJob.TargetTimestamp,
				}

				continue
			}

			if objAttrs.Metadata == nil {
				outJobs <- GetVersionOutJob{
					Err:             errors.New("metadata was blank"),
					RegionName:      inJob.RegionName,
					RealmSlug:       inJob.RealmSlug,
					TargetTimestamp: inJob.TargetTimestamp,
				}

				continue
			}

			version, ok := objAttrs.Metadata["version_id"]
			if !ok {
				outJobs <- GetVersionOutJob{
					Err:             errors.New("metadata did not have version_id"),
					RegionName:      inJob.RegionName,
					RealmSlug:       inJob.RealmSlug,
					TargetTimestamp: inJob.TargetTimestamp,
				}

				continue
			}
			if version == "" {
				outJobs <- GetVersionOutJob{
					Err:             errors.New("version_id was blank"),
					RegionName:      inJob.RegionName,
					RealmSlug:       inJob.RealmSlug,
					TargetTimestamp: inJob.TargetTimestamp,
				}

				continue
			}

			outJobs <- GetVersionOutJob{
				Err:             nil,
				RegionName:      inJob.RegionName,
				RealmSlug:       inJob.RealmSlug,
				TargetTimestamp: inJob.TargetTimestamp,
				Version:         version,
			}

		}
	}
	postWork := func() {
		close(outJobs)
	}
	util.Work(16, worker, postWork)

	// queueing it up
	go func() {
		for regionName, realmTimestamps := range timestamps {
			for realmSlug, timestamps := range realmTimestamps {
				for _, timestamp := range timestamps {
					inJobs <- GetVersionsInJob{
						RegionName:      regionName,
						RealmSlug:       realmSlug,
						TargetTimestamp: timestamp,
					}
				}
			}
		}

		close(inJobs)
	}()

	// going over results
	versions := sotah.PricelistHistoryVersions{}
	for outJob := range outJobs {
		if outJob.Err != nil {
			return sotah.PricelistHistoryVersions{}, outJob.Err
		}

		versions = versions.Insert(outJob.RegionName, outJob.RealmSlug, outJob.TargetTimestamp, outJob.Version)
	}

	return versions, nil
}

func (b PricelistHistoriesBaseV2) GetAllTimestamps(
	regionRealms map[blizzard.RegionName]sotah.Realms,
	bkt *storage.BucketHandle,
) (RegionRealmTimestamps, error) {
	out := make(chan GetTimestampsJob)
	in := make(chan sotah.Realm)

	// spinning up workers
	worker := func() {
		for realm := range in {
			timestamps, err := b.GetTimestamps(realm, bkt)
			if err != nil {
				out <- GetTimestampsJob{
					Err:   err,
					Realm: realm,
				}

				continue
			}

			out <- GetTimestampsJob{
				Err:        nil,
				Realm:      realm,
				Timestamps: timestamps,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(16, worker, postWork)

	// queueing it up
	go func() {
		for _, realms := range regionRealms {
			for _, realm := range realms {
				in <- realm
			}
		}

		close(in)
	}()

	// going over results
	results := RegionRealmTimestamps{}
	for job := range out {
		if job.Err != nil {
			return RegionRealmTimestamps{}, job.Err
		}

		regionName := job.Realm.Region.Name
		if _, ok := results[regionName]; !ok {
			results[regionName] = RealmTimestamps{}
		}

		results[regionName][job.Realm.Slug] = job.Timestamps
	}

	return results, nil
}

func (b PricelistHistoriesBaseV2) GetAllExpiredTimestamps(
	regionRealms map[blizzard.RegionName]sotah.Realms,
	bkt *storage.BucketHandle,
) (RegionRealmTimestamps, error) {
	regionRealmTimestamps, err := b.GetAllTimestamps(regionRealms, bkt)
	if err != nil {
		return RegionRealmTimestamps{}, err
	}

	out := RegionRealmTimestamps{}
	limit := sotah.NormalizeTargetDate(time.Now()).AddDate(0, 0, -14)
	for regionName, realmTimestamps := range regionRealmTimestamps {
		for realmSlug, timestamps := range realmTimestamps {
			for _, timestamp := range timestamps {
				targetTime := time.Unix(int64(timestamp), 0)
				if targetTime.After(limit) {
					continue
				}

				if _, ok := out[regionName]; !ok {
					out[regionName] = RealmTimestamps{}
				}
				if _, ok := out[regionName][realmSlug]; !ok {
					out[regionName][realmSlug] = []sotah.UnixTimestamp{}
				}

				out[regionName][realmSlug] = append(out[regionName][realmSlug], timestamp)
			}
		}
	}

	return out, nil
}

func (b PricelistHistoriesBaseV2) GetTimestamps(realm sotah.Realm, bkt *storage.BucketHandle) ([]sotah.UnixTimestamp, error) {
	prefix := fmt.Sprintf("%s/%s/", realm.Region.Name, realm.Slug)
	it := bkt.Objects(b.client.Context, &storage.Query{Prefix: prefix})
	out := []sotah.UnixTimestamp{}
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}

			if err != nil {
				return []sotah.UnixTimestamp{}, err
			}
		}

		targetTimestamp, err := strconv.Atoi(objAttrs.Name[len(prefix):(len(objAttrs.Name) - len(".txt.gz"))])
		if err != nil {
			return []sotah.UnixTimestamp{}, err
		}

		out = append(out, sotah.UnixTimestamp(targetTimestamp))
	}

	return out, nil
}
