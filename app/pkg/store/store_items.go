package store

import (
	"cloud.google.com/go/storage"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/util"
	"google.golang.org/api/iterator"
	"io/ioutil"
	"strconv"
	"strings"
)

const itemsBucketName = "sotah-items"

func (sto Store) getItemsBucket() *storage.BucketHandle {
	return sto.client.Bucket(itemsBucketName)
}

func (sto Store) createItemsBucket() (*storage.BucketHandle, error) {
	bkt := sto.getItemsBucket()
	err := bkt.Create(sto.Context, sto.projectID, &storage.BucketAttrs{
		StorageClass: "REGIONAL",
		Location:     "us-east1",
	})
	if err != nil {
		return nil, err
	}

	return bkt, nil
}

func (sto Store) itemsBucketExists() (bool, error) {
	_, err := sto.getItemsBucket().Attrs(sto.Context)
	if err != nil {
		if err != storage.ErrBucketNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (sto Store) resolveItemsBucket() (*storage.BucketHandle, error) {
	exists, err := sto.itemsBucketExists()
	if err != nil {
		return nil, err
	}

	if !exists {
		return sto.createItemsBucket()
	}

	return sto.getItemsBucket(), nil
}
func (sto Store) getItemObjectName(ID blizzard.ItemID) string {
	return fmt.Sprintf("%d.json.gz", ID)
}

func (sto Store) WriteItem(ID blizzard.ItemID, body []byte) error {
	logging.WithFields(logrus.Fields{
		"ID":     ID,
		"length": len(body),
	}).Debug("Writing item to gcloud storage")

	// writing it out
	obj := sto.itemsBucket.Object(sto.getItemObjectName(ID))
	wc := obj.NewWriter(sto.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	if _, err := wc.Write(body); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}

func (sto Store) ItemExists(ID blizzard.ItemID) (bool, error) {
	_, err := sto.itemsBucket.Object(sto.getItemObjectName(ID)).Attrs(sto.Context)
	if err != nil {
		if err != storage.ErrObjectNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (sto Store) GetItemObject(ID blizzard.ItemID) *storage.ObjectHandle {
	return sto.itemsBucket.Object(sto.getItemObjectName(ID))
}

func (sto Store) exportItem(ID blizzard.ItemID) ([]byte, error) {
	reader, err := sto.itemsBucket.Object(sto.getItemObjectName(ID)).NewReader(sto.Context)
	if err != nil {
		return []byte{}, err
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

type exportItemsJob struct {
	err  error
	ID   blizzard.ItemID
	data []byte
}

func (sto Store) exportItems() chan exportItemsJob {
	// establishing channels
	out := make(chan exportItemsJob)
	in := make(chan blizzard.ItemID)

	// spinning up the workers
	worker := func() {
		for ID := range in {
			data, err := sto.exportItem(ID)
			out <- exportItemsJob{err, ID, data}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing up
	go func() {
		i := 0
		it := sto.itemsBucket.Objects(sto.Context, nil)
		for {
			if i == 0 || i%5000 == 0 {
				logging.WithField("count", i).Debug("Exported items from Store")
			}

			objAttrs, err := it.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}

				logging.WithField("error", err.Error()).Error("Failed to iterate over item objects")

				continue
			}

			s := strings.Split(objAttrs.Name, ".")
			ID, err := strconv.Atoi(s[0])
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error": err.Error(),
					"name":  objAttrs.Name,
				}).Error("Failed to parse object name")

				continue
			}

			in <- blizzard.ItemID(ID)
			i++
		}

		close(in)
	}()

	return out
}
