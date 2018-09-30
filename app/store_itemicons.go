package main

import (
	"fmt"

	storage "cloud.google.com/go/storage"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/util"
	"github.com/sirupsen/logrus"
)

const storeItemIconURLFormat = "https://storage.googleapis.com/%s/%s"

func (sto store) getStoreItemIconURLFunc(obj *storage.ObjectHandle) (string, error) {
	bktAttrs, err := sto.itemIconsBucket.Attrs(sto.context)
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
	logging.WithFields(logrus.Fields{
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

	return sto.getStoreItemIconURLFunc(obj)
}

func (sto store) itemIconExists(iconName string) (bool, error) {
	_, err := sto.itemIconsBucket.Object(sto.getItemIconObjectName(iconName)).Attrs(sto.context)
	if err != nil {
		if err != storage.ErrObjectNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

type storeSyncItemIconsJob struct {
	err      error
	iconName string
	iconURL  string
}

func (sto store) syncItemIcons(iconNames []string, res resolver) (chan storeSyncItemIconsJob, error) {
	// establishing channels
	out := make(chan storeSyncItemIconsJob)
	in := make(chan string)

	// spinning up the workers
	worker := func() {
		for iconName := range in {
			iconURL, err := sto.syncItemIcon(iconName, res)
			out <- storeSyncItemIconsJob{err, iconName, iconURL}
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

func (sto store) syncItemIcon(iconName string, res resolver) (string, error) {
	bkt := sto.itemIconsBucket

	exists, err := sto.itemIconExists(iconName)
	if err != nil {
		return "", err
	}

	if exists {
		return sto.getStoreItemIconURLFunc(bkt.Object(sto.getItemIconObjectName(iconName)))
	}

	body, err := util.Download(res.getItemIconURL(iconName))
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":    err.Error(),
			"iconName": iconName,
		}).Error("Failed to sync item icon (gcloud store)")

		return "", nil
	}

	return sto.writeItemIcon(bkt, iconName, body)
}
