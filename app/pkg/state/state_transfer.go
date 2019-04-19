package state

import (
	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
)

type TransferStateConfig struct {
	InProjectId  string
	InBucketName string

	OutProjectId  string
	OutBucketName string
}

func NewTransferState(config TransferStateConfig) (TransferState, error) {
	// establishing an initial state
	transferState := TransferState{
		State: NewState(uuid.NewV4(), true),
	}

	inStoreClient, err := store.NewClient(config.InProjectId)
	if err != nil {
		return TransferState{}, err
	}
	transferState.InTransferBase = store.NewTransferBase(inStoreClient, "us-central1", config.InBucketName)

	inBucket, err := transferState.InTransferBase.GetFirmBucket()
	if err != nil {
		return TransferState{}, err
	}
	transferState.InBucket = inBucket

	outStoreClient, err := store.NewClient(config.OutProjectId)
	if err != nil {
		return TransferState{}, err
	}
	transferState.OutTransferBase = store.NewTransferBase(outStoreClient, "us-central1", config.OutBucketName)

	outBucket, err := transferState.OutTransferBase.GetFirmBucket()
	if err != nil {
		return TransferState{}, err
	}
	transferState.OutBucket = outBucket

	return transferState, nil
}

type TransferState struct {
	State

	InTransferBase store.TransferBase
	InBucket       *storage.BucketHandle

	OutTransferBase store.TransferBase
	OutBucket       *storage.BucketHandle
}

func (transferState TransferState) Run() error {
	return nil
}
