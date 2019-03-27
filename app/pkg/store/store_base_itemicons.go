package store

import (
	"encoding/json"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
)

func NewItemIconsBase(c Client, location string) ItemIconsBase {
	return ItemIconsBase{base{client: c, location: location}}
}

type ItemIconsBase struct {
	base
}

func (b ItemIconsBase) getBucketName() string {
	return "item-icons"
}

func (b ItemIconsBase) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b ItemIconsBase) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b ItemIconsBase) resolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b ItemIconsBase) getObjectName(name string) string {
	return fmt.Sprintf("%s.jpg", name)
}

func (b ItemIconsBase) GetObject(name string, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(name), bkt)
}

func (b ItemIconsBase) GetFirmObject(name string, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(b.getObjectName(name), bkt)
}

func NewIconItemsPayloads(data string) (IconItemsPayloads, error) {
	var out IconItemsPayloads
	if err := json.Unmarshal([]byte(data), &out); err != nil {
		return IconItemsPayloads{}, err
	}

	return out, nil
}

type IconItemsPayloads []IconItemsPayload

func (d IconItemsPayloads) EncodeForDelivery() (string, error) {
	jsonEncoded, err := json.Marshal(d)
	if err != nil {
		return "", err
	}

	return string(jsonEncoded), nil
}

type IconItemsPayload struct {
	Name string
	Ids  blizzard.ItemIds
}
