package main

import (
	"fmt"
	"io/ioutil"

	storage "cloud.google.com/go/storage"
	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/util"
	log "github.com/sirupsen/logrus"
)

const itemsBucketName = "item"

func (sto store) getItemsBucket() *storage.BucketHandle {
	return sto.client.Bucket(itemsBucketName)
}

func (sto store) createItemsBucket() (*storage.BucketHandle, error) {
	bkt := sto.getItemsBucket()
	err := bkt.Create(sto.context, sto.projectID, &storage.BucketAttrs{
		StorageClass: "REGIONAL",
		Location:     "us-east1",
	})
	if err != nil {
		return nil, err
	}

	return bkt, nil
}

func (sto store) itemsBucketExists() (bool, error) {
	_, err := sto.getItemsBucket().Attrs(sto.context)
	if err != nil {
		if err != storage.ErrBucketNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (sto store) resolveItemsBucket() (*storage.BucketHandle, error) {
	exists, err := sto.itemsBucketExists()
	if err != nil {
		return nil, err
	}

	if !exists {
		return sto.createItemsBucket()
	}

	return sto.getItemsBucket(), nil
}
func (sto store) getItemObjectName(ID blizzard.ItemID) string {
	return fmt.Sprintf("%d.json.gz", ID)
}

func (sto store) writeItem(bkt *storage.BucketHandle, ID blizzard.ItemID, body []byte) error {
	log.WithFields(log.Fields{
		"ID":     ID,
		"length": len(body),
	}).Debug("Writing item to gcloud storage")

	// writing it out
	obj := bkt.Object(sto.getItemObjectName(ID))
	wc := obj.NewWriter(sto.context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	wc.Write(body)
	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}

func (sto store) itemExists(bkt *storage.BucketHandle, ID blizzard.ItemID) (bool, error) {
	_, err := bkt.Object(sto.getItemObjectName(ID)).Attrs(sto.context)
	if err != nil {
		if err != storage.ErrObjectNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (sto store) getItems(IDs []blizzard.ItemID, res resolver) (chan getItemsJob, error) {
	// resolving the item-icons bucket
	bkt, err := sto.resolveItemsBucket()
	if err != nil {
		return nil, err
	}

	// establishing channels
	out := make(chan getItemsJob)
	in := make(chan blizzard.ItemID)

	// spinning up the workers
	worker := func() {
		for ID := range in {
			itemValue, err := sto.getItem(bkt, ID, res)
			out <- getItemsJob{err, ID, itemValue}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing up
	go func() {
		for _, ID := range IDs {
			in <- ID
		}

		close(in)
	}()

	return out, nil
}

func (sto store) getItem(bkt *storage.BucketHandle, ID blizzard.ItemID, res resolver) (blizzard.Item, error) {
	exists, err := sto.itemExists(bkt, ID)
	if err != nil {
		return blizzard.Item{}, err
	}

	if exists {
		obj := bkt.Object(sto.getItemObjectName(ID))

		reader, err := obj.NewReader(sto.context)
		if err != nil {
			return blizzard.Item{}, err
		}
		defer reader.Close()

		body, err := ioutil.ReadAll(reader)
		if err != nil {
			return blizzard.Item{}, err
		}

		return blizzard.NewItem(body)
	}

	primaryRegion, err := res.config.Regions.getPrimaryRegion()
	if err != nil {
		return blizzard.Item{}, err
	}

	uri, err := res.appendAPIKey(res.getItemURL(primaryRegion.Hostname, ID))
	if err != nil {
		return blizzard.Item{}, err
	}

	log.WithField("item", ID).Info("Fetching item")

	item, resp, err := blizzard.NewItemFromHTTP(uri)
	if err != nil {
		return blizzard.Item{}, err
	}
	if err := res.messenger.publishPlanMetaMetric(resp); err != nil {
		return blizzard.Item{}, err
	}

	encodedBody, err := util.GzipEncode(resp.Body)
	if err != nil {
		return blizzard.Item{}, err
	}

	if err := sto.writeItem(bkt, ID, encodedBody); err != nil {
		return blizzard.Item{}, err
	}

	return item, nil
}
