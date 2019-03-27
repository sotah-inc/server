package store

import (
	"fmt"
	"io/ioutil"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
)

func NewItemsBase(c Client, location string) ItemsBase {
	return ItemsBase{base{client: c, location: location}}
}

type ItemsBase struct {
	base
}

func (b ItemsBase) getBucketName() string {
	return "sotah-items"
}

func (b ItemsBase) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b ItemsBase) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b ItemsBase) resolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b ItemsBase) getObjectName(id blizzard.ItemID) string {
	return fmt.Sprintf("%d.json.gz", id)
}

func (b ItemsBase) GetObject(id blizzard.ItemID, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(id), bkt)
}

func (b ItemsBase) GetFirmObject(id blizzard.ItemID, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(b.getObjectName(id), bkt)
}

func (b ItemsBase) ObjectExists(id blizzard.ItemID, bkt *storage.BucketHandle) (bool, error) {
	return b.base.ObjectExists(b.GetObject(id, bkt))
}

func (b ItemsBase) NewItem(obj *storage.ObjectHandle) (sotah.Item, error) {
	reader, err := obj.NewReader(b.client.Context)
	if err != nil {
		return sotah.Item{}, err
	}
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		return sotah.Item{}, err
	}

	return sotah.NewItem(body)
}

type GetItemsOutJob struct {
	Err             error
	Id              blizzard.ItemID
	GzipEncodedData []byte
}

func (job GetItemsOutJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error": job.Err.Error(),
		"id":    job.Id,
	}
}

func (b ItemsBase) GetItems(ids blizzard.ItemIds, bkt *storage.BucketHandle) chan GetItemsOutJob {
	// spinning up workers
	in := make(chan blizzard.ItemID)
	out := make(chan GetItemsOutJob)
	worker := func() {
		for id := range in {
			obj, err := b.GetFirmObject(id, bkt)
			if err != nil {
				out <- GetItemsOutJob{
					Err: err,
					Id:  id,
				}

				continue
			}

			reader, err := obj.ReadCompressed(true).NewReader(b.client.Context)
			if err != nil {
				out <- GetItemsOutJob{
					Err: err,
					Id:  id,
				}

				continue
			}

			gzipEncodedData, err := ioutil.ReadAll(reader)
			if err != nil {
				out <- GetItemsOutJob{
					Err: err,
					Id:  id,
				}

				continue
			}

			out <- GetItemsOutJob{
				Err:             nil,
				Id:              id,
				GzipEncodedData: gzipEncodedData,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(16, worker, postWork)

	// enqueueing it up
	go func() {
		for _, id := range ids {
			in <- id
		}

		close(in)
	}()

	return out
}
