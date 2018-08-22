package main

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	storage "cloud.google.com/go/storage"
	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/util"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

const itemsBucketName = "sotah-items"

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
			itemValue, iconURL, err := sto.getItem(bkt, ID, res)
			out <- getItemsJob{err, ID, itemValue, iconURL}
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

func (sto store) getItem(bkt *storage.BucketHandle, ID blizzard.ItemID, res resolver) (blizzard.Item, string, error) {
	exists, err := sto.itemExists(bkt, ID)
	if err != nil {
		return blizzard.Item{}, "", err
	}

	if exists {
		return sto.loadItem(bkt, ID, res)
	}

	primaryRegion, err := res.config.Regions.getPrimaryRegion()
	if err != nil {
		return blizzard.Item{}, "", err
	}

	uri, err := res.appendAPIKey(res.getItemURL(primaryRegion.Hostname, ID))
	if err != nil {
		return blizzard.Item{}, "", err
	}

	log.WithField("item", ID).Info("Fetching item")

	itemValue, resp, err := blizzard.NewItemFromHTTP(uri)
	if err != nil {
		return blizzard.Item{}, "", err
	}
	if err := res.messenger.publishPlanMetaMetric(resp); err != nil {
		return blizzard.Item{}, "", err
	}

	encodedBody, err := util.GzipEncode(resp.Body)
	if err != nil {
		return blizzard.Item{}, "", err
	}

	if err := sto.writeItem(bkt, ID, encodedBody); err != nil {
		return blizzard.Item{}, "", err
	}

	return sto.fulfilItemIcon(itemValue, res)
}

func (sto store) loadItem(bkt *storage.BucketHandle, ID blizzard.ItemID, res resolver) (blizzard.Item, string, error) {
	obj := bkt.Object(sto.getItemObjectName(ID))

	reader, err := obj.NewReader(sto.context)
	if err != nil {
		return blizzard.Item{}, "", err
	}
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		return blizzard.Item{}, "", err
	}

	itemValue, err := blizzard.NewItem(body)
	if err != nil {
		return blizzard.Item{}, "", err
	}

	return sto.fulfilItemIcon(itemValue, res)
}

func (sto store) loadItems(res resolver) (chan loadItemsJob, error) {
	// resolving the item-icons bucket
	bkt, err := sto.resolveItemsBucket()
	if err != nil {
		return nil, err
	}

	// establishing channels
	out := make(chan loadItemsJob)
	in := make(chan blizzard.ItemID)

	// spinning up the workers
	worker := func() {
		for ID := range in {
			itemValue, iconURL, err := sto.loadItem(bkt, ID, res)
			out <- loadItemsJob{err, strconv.Itoa(int(ID)), itemValue, iconURL}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing up
	go func() {
		i := 0
		it := bkt.Objects(sto.context, nil)
		for {
			if i == 0 || i%5000 == 0 {
				log.WithField("count", i).Debug("Loaded items from store")
			}

			objAttrs, err := it.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}

				log.WithField("error", err.Error()).Info("Failed to iterate over item objects")

				continue
			}

			s := strings.Split(objAttrs.Name, ".")
			ID, err := strconv.Atoi(s[0])
			if err != nil {
				log.WithFields(log.Fields{
					"error": err.Error(),
					"name":  objAttrs.Name,
				}).Info("Failed to parse object name")

				continue
			}

			in <- blizzard.ItemID(ID)
			i++
		}

		close(in)
	}()

	return out, nil
}
