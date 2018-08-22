package main

import (
	"fmt"

	storage "cloud.google.com/go/storage"
	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/util"
	log "github.com/sirupsen/logrus"
)

const storeItemIconURLFormat = "https://storage.googleapis.com/%s/%s"

func (sto store) getStoreItemIconURLFunc(bkt *storage.BucketHandle, obj *storage.ObjectHandle) (string, error) {
	bktAttrs, err := bkt.Attrs(sto.context)
	if err != nil {
		return "", err
	}

	objAttrs, err := obj.Attrs(sto.context)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(storeItemIconURLFormat, bktAttrs.Name, objAttrs.Name), nil
}

const itemIconsBucketName = "item-icons"

func (sto store) getItemIconsBucket() *storage.BucketHandle {
	return sto.client.Bucket(itemIconsBucketName)
}

func (sto store) createItemIconsBucket() (*storage.BucketHandle, error) {
	bkt := sto.getItemIconsBucket()
	err := bkt.Create(sto.context, sto.projectID, &storage.BucketAttrs{
		StorageClass: "REGIONAL",
		Location:     "us-east1",
	})
	if err != nil {
		return nil, err
	}

	return bkt, nil
}

func (sto store) itemIconsBucketExists() (bool, error) {
	_, err := sto.getItemIconsBucket().Attrs(sto.context)
	if err != nil {
		if err != storage.ErrBucketNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (sto store) resolveItemIconsBucket() (*storage.BucketHandle, error) {
	exists, err := sto.itemIconsBucketExists()
	if err != nil {
		return nil, err
	}

	if !exists {
		return sto.createItemIconsBucket()
	}

	return sto.getItemIconsBucket(), nil
}
func (sto store) getItemIconObjectName(iconName string) string {
	return fmt.Sprintf("%s.jpg", iconName)
}

func (sto store) writeItemIcon(bkt *storage.BucketHandle, iconName string, body []byte) (string, error) {
	log.WithFields(log.Fields{
		"icon":   iconName,
		"length": len(body),
	}).Debug("Writing item-icon to gcloud storage")

	// writing it out
	obj := bkt.Object(sto.getItemIconObjectName(iconName))
	wc := obj.NewWriter(sto.context)
	wc.ContentType = "image/jpeg"
	wc.Write(body)
	if err := wc.Close(); err != nil {
		return "", err
	}

	// setting acl of item-icon object to public
	acl := obj.ACL()
	if err := acl.Set(sto.context, storage.AllUsers, storage.RoleReader); err != nil {
		return "", err
	}

	return sto.getStoreItemIconURLFunc(bkt, obj)
}

func (sto store) itemIconExists(bkt *storage.BucketHandle, iconName string) (bool, error) {
	_, err := bkt.Object(sto.getItemIconObjectName(iconName)).Attrs(sto.context)
	if err != nil {
		if err != storage.ErrObjectNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

type syncItemIconStoreJob struct {
	err      error
	iconName string
	iconURL  string
}

func (sto store) syncItemIcons(iconNames []string, res resolver) (chan syncItemIconStoreJob, error) {
	// resolving the item-icons bucket
	bkt, err := sto.resolveItemIconsBucket()
	if err != nil {
		return nil, err
	}

	// establishing channels
	out := make(chan syncItemIconStoreJob)
	in := make(chan string)

	// spinning up the workers
	worker := func() {
		for iconName := range in {
			iconURL, err := sto.syncItemIcon(bkt, iconName, res)
			out <- syncItemIconStoreJob{err, iconName, iconURL}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing up
	go func() {
		for _, iconName := range iconNames {
			in <- iconName
		}

		close(in)
	}()

	return out, nil
}

func (sto store) syncItemIcon(bkt *storage.BucketHandle, iconName string, res resolver) (string, error) {
	exists, err := sto.itemIconExists(bkt, iconName)
	if err != nil {
		return "", err
	}

	if exists {
		return sto.getStoreItemIconURLFunc(bkt, bkt.Object(sto.getItemIconObjectName(iconName)))
	}

	body, err := util.Download(res.getItemIconURL(iconName))
	if err != nil {
		log.WithFields(log.Fields{
			"iconName": iconName,
			"error":    err.Error(),
		}).Info("Failed to sync item icon (gcloud store)")

		return "", nil
	}

	return sto.writeItemIcon(bkt, iconName, body)
}

func (sto store) fulfilItemIcon(itemValue blizzard.Item, res resolver) (blizzard.Item, string, error) {
	if itemValue.Icon == "" {
		return itemValue, "", nil
	}

	itemIconBucket, err := sto.resolveItemIconsBucket()
	if err != nil {
		return blizzard.Item{}, "", err
	}

	iconExists, err := sto.itemIconExists(itemIconBucket, itemValue.Icon)
	if err != nil {
		return blizzard.Item{}, "", err
	}
	if !iconExists {
		iconURL, err := sto.syncItemIcon(itemIconBucket, itemValue.Icon, res)
		if err != nil {
			return blizzard.Item{}, "", err
		}

		return itemValue, iconURL, nil
	}

	iconURL, err := sto.getStoreItemIconURLFunc(itemIconBucket, itemIconBucket.Object(sto.getItemIconObjectName(itemValue.Icon)))
	if err != nil {
		return blizzard.Item{}, "", err
	}

	return itemValue, iconURL, nil
}
