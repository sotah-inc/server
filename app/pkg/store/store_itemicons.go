package store

import (
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"
)

const storeItemIconURLFormat = "https://storage.googleapis.com/%s/%s"

func (sto Store) getStoreItemIconURLFunc(obj *storage.ObjectHandle) (string, error) {
	bktAttrs, err := sto.itemIconsBucket.Attrs(sto.Context)
	if err != nil {
		return "", err
	}

	objAttrs, err := obj.Attrs(sto.Context)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(storeItemIconURLFormat, bktAttrs.Name, objAttrs.Name), nil
}

const itemIconsBucketName = "item-icons"

func (sto Store) getItemIconsBucket() *storage.BucketHandle {
	return sto.client.Bucket(itemIconsBucketName)
}

func (sto Store) createItemIconsBucket() (*storage.BucketHandle, error) {
	bkt := sto.getItemIconsBucket()
	err := bkt.Create(sto.Context, sto.projectID, &storage.BucketAttrs{
		StorageClass: "REGIONAL",
		Location:     "us-east1",
	})
	if err != nil {
		return nil, err
	}

	return bkt, nil
}

func (sto Store) itemIconsBucketExists() (bool, error) {
	_, err := sto.getItemIconsBucket().Attrs(sto.Context)
	if err != nil {
		if err != storage.ErrBucketNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (sto Store) resolveItemIconsBucket() (*storage.BucketHandle, error) {
	exists, err := sto.itemIconsBucketExists()
	if err != nil {
		return nil, err
	}

	if !exists {
		return sto.createItemIconsBucket()
	}

	return sto.getItemIconsBucket(), nil
}
func (sto Store) getItemIconObjectName(iconName string) string {
	return fmt.Sprintf("%s.jpg", iconName)
}

func (sto Store) writeItemIcon(bkt *storage.BucketHandle, iconName string, body []byte) (string, error) {
	logging.WithFields(logrus.Fields{
		"icon":   iconName,
		"length": len(body),
	}).Debug("Writing item-icon to gcloud storage")

	// writing it out
	obj := bkt.Object(sto.getItemIconObjectName(iconName))
	wc := obj.NewWriter(sto.Context)
	wc.ContentType = "image/jpeg"
	wc.Write(body)
	if err := wc.Close(); err != nil {
		return "", err
	}

	// setting acl of item-icon object to public
	acl := obj.ACL()
	if err := acl.Set(sto.Context, storage.AllUsers, storage.RoleReader); err != nil {
		return "", err
	}

	return sto.getStoreItemIconURLFunc(obj)
}

func (sto Store) itemIconExists(iconName string) (bool, error) {
	_, err := sto.itemIconsBucket.Object(sto.getItemIconObjectName(iconName)).Attrs(sto.Context)
	if err != nil {
		if err != storage.ErrObjectNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}
