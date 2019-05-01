package store

import (
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/store/regions"
)

func NewLiveAuctionsBase(c Client, location regions.Region, version gameversions.GameVersion) LiveAuctionsBase {
	return LiveAuctionsBase{
		base{client: c, location: location},
		version,
	}
}

type LiveAuctionsBase struct {
	base
	GameVersion gameversions.GameVersion
}

func (b LiveAuctionsBase) getBucketName() string {
	return "sotah-live-auctions"
}

func (b LiveAuctionsBase) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b LiveAuctionsBase) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b LiveAuctionsBase) resolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b LiveAuctionsBase) getObjectName(realm sotah.Realm) string {
	return fmt.Sprintf("%s/%s/%s.json.gz", b.GameVersion, realm.Region.Name, realm.Slug)
}

func (b LiveAuctionsBase) GetObject(realm sotah.Realm, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(realm), bkt)
}

func (b LiveAuctionsBase) GetFirmObject(realm sotah.Realm, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(b.getObjectName(realm), bkt)
}

func (b LiveAuctionsBase) Handle(aucs blizzard.Auctions, realm sotah.Realm, bkt *storage.BucketHandle) error {
	// encoding auctions in the appropriate format
	gzipEncodedBody, err := sotah.NewMiniAuctionListFromMiniAuctions(sotah.NewMiniAuctions(aucs)).EncodeForDatabase()
	if err != nil {
		return err
	}

	// writing it out to the gcloud object
	wc := b.GetObject(realm, bkt).NewWriter(b.client.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	if err := b.Write(wc, gzipEncodedBody); err != nil {
		return err
	}

	return nil
}
