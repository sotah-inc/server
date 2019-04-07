package store

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/store/regions"
	"github.com/sotah-inc/server/app/pkg/util"
	"google.golang.org/api/iterator"
)

func NewRealmsBase(c Client, location regions.Region, version gameversions.GameVersion) RealmsBase {
	return RealmsBase{
		base{client: c, location: location},
		version,
	}
}

type RealmsBase struct {
	base
	GameVersion gameversions.GameVersion
}

func (b RealmsBase) getBucketName() string {
	return "sotah-realms"
}

func (b RealmsBase) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b RealmsBase) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b RealmsBase) GetObjectName(regionName blizzard.RegionName, realmSlug blizzard.RealmSlug) string {
	return fmt.Sprintf("%s/%s/%s.json.gz", b.GameVersion, regionName, realmSlug)
}

func (b RealmsBase) GetObject(
	regionName blizzard.RegionName,
	realmSlug blizzard.RealmSlug,
	bkt *storage.BucketHandle,
) *storage.ObjectHandle {
	return b.base.getObject(b.GetObjectName(regionName, realmSlug), bkt)
}

func (b RealmsBase) GetFirmObject(regionName blizzard.RegionName, realmSlug blizzard.RealmSlug, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(b.GetObjectName(regionName, realmSlug), bkt)
}

func (b RealmsBase) NewRealm(obj *storage.ObjectHandle) (sotah.Realm, error) {
	reader, err := obj.NewReader(b.client.Context)
	if err != nil {
		return sotah.Realm{}, err
	}

	gzipDecodedData, err := ioutil.ReadAll(reader)
	if err != nil {
		return sotah.Realm{}, err
	}

	var out sotah.Realm
	if err := json.Unmarshal(gzipDecodedData, &out); err != nil {
		return sotah.Realm{}, err
	}

	return out, nil
}

type GetRealmsOutJob struct {
	Err   error
	Realm sotah.Realm
}

func (b RealmsBase) GetRealms(regionName blizzard.RegionName, bkt *storage.BucketHandle) (sotah.Realms, error) {
	// spinning up the workers
	in := make(chan string)
	out := make(chan GetRealmsOutJob)
	worker := func() {
		for objName := range in {
			realm, err := b.NewRealm(b.getObject(objName, bkt))
			if err != nil {
				out <- GetRealmsOutJob{
					Err:   err,
					Realm: sotah.Realm{},
				}

				continue
			}

			out <- GetRealmsOutJob{
				Err:   nil,
				Realm: realm,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing it up
	prefix := fmt.Sprintf("%s/", regionName)
	it := bkt.Objects(b.client.Context, &storage.Query{Prefix: prefix})
	go func() {
		for {
			objAttrs, err := it.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}

				logging.WithField("error", err.Error()).Error("Failed to iterate to next")

				break
			}

			in <- objAttrs.Name
		}

		close(in)
	}()

	// waiting for it to drain out
	results := sotah.Realms{}
	for job := range out {
		if job.Err != nil {
			return sotah.Realms{}, job.Err
		}

		results = append(results, job.Realm)
	}

	return results, nil
}

func (b ItemsBase) WriteRealm(obj *storage.ObjectHandle, realm sotah.Realm) error {
	jsonEncoded, err := json.Marshal(realm)
	if err != nil {
		return err
	}

	gzipEncodedBody, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return err
	}

	wc := obj.NewWriter(b.client.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	return b.Write(wc, gzipEncodedBody)
}
