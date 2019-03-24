package store

import (
	"errors"
	"fmt"

	"github.com/sotah-inc/server/app/pkg/util"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"
)

const storeItemIconURLFormat = "https://storage.googleapis.com/%s/%s"

func (sto Client) GetStoreItemIconURLFunc(obj *storage.ObjectHandle) (string, error) {
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

func (sto Client) getItemIconsBucket() *storage.BucketHandle {
	return sto.client.Bucket(itemIconsBucketName)
}

func (sto Client) createItemIconsBucket() (*storage.BucketHandle, error) {
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

func (sto Client) itemIconsBucketExists() (bool, error) {
	_, err := sto.getItemIconsBucket().Attrs(sto.Context)
	if err != nil {
		if err != storage.ErrBucketNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (sto Client) resolveItemIconsBucket() (*storage.BucketHandle, error) {
	if sto.itemIconsBucket != nil {
		return sto.itemIconsBucket, nil
	}

	exists, err := sto.itemIconsBucketExists()
	if err != nil {
		return nil, err
	}

	if !exists {
		return sto.createItemIconsBucket()
	}

	sto.itemIconsBucket = sto.getItemIconsBucket()
	return sto.itemIconsBucket, nil
}
func (sto Client) getItemIconObjectName(iconName string) string {
	return fmt.Sprintf("%s.jpg", iconName)
}

func (sto Client) GetItemIconObject(iconName string) (*storage.ObjectHandle, error) {
	exists, err := sto.ItemIconExists(iconName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("item icon object does not exist")
	}

	bkt, err := sto.resolveItemIconsBucket()
	if err != nil {
		return nil, err
	}

	return bkt.Object(sto.getItemIconObjectName(iconName)), nil
}

func (sto Client) WriteItemIcon(iconName string, body []byte) (string, error) {
	bkt, err := sto.resolveItemIconsBucket()
	if err != nil {
		return "", err
	}

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

	return sto.GetStoreItemIconURLFunc(obj)
}

func (sto Client) ItemIconExists(iconName string) (bool, error) {
	_, err := sto.itemIconsBucket.Object(sto.getItemIconObjectName(iconName)).Attrs(sto.Context)
	if err != nil {
		if err != storage.ErrObjectNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

type PersistItemIconsInJob struct {
	IconName string
	Data     []byte
}

type PersistItemIconsOutJob struct {
	Err      error
	IconName string
	IconURL  string
}

func (sto Client) PersistItemIcons(in chan PersistItemIconsInJob) chan PersistItemIconsOutJob {
	// forming channels
	out := make(chan PersistItemIconsOutJob)

	// spinning up the workers for fetching items
	worker := func() {
		for job := range in {
			iconURL, err := sto.WriteItemIcon(job.IconName, job.Data)
			out <- PersistItemIconsOutJob{err, job.IconName, iconURL}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	return out
}
