package state

import (
	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
	"google.golang.org/api/iterator"
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
	transferState.InStoreClient = inStoreClient
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
	transferState.OutStoreClient = outStoreClient
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

	InStoreClient  store.Client
	InTransferBase store.TransferBase
	InBucket       *storage.BucketHandle

	OutStoreClient  store.Client
	OutTransferBase store.TransferBase
	OutBucket       *storage.BucketHandle
}

func (transferState TransferState) Copy(name string) error {
	src, err := transferState.InTransferBase.GetFirmObject(name, transferState.InBucket)
	if err != nil {
		return err
	}

	dst := transferState.OutTransferBase.GetObject(name, transferState.OutBucket)
	destinationExists, err := transferState.OutTransferBase.ObjectExists(dst)
	if err != nil {
		return err
	}
	if destinationExists {
		return nil
	}

	copier := dst.CopierFrom(src)
	if _, err := copier.Run(transferState.OutStoreClient.Context); err != nil {
		return err
	}

	return nil
}

func (transferState TransferState) Run() error {
	total := 0
	it := transferState.InBucket.Objects(transferState.InStoreClient.Context, nil)
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}

			return err
		}

		logging.WithField("name", objAttrs.Name).Info("Found object")
		total++
	}

	logging.WithField("total", total).Info("Found objects")

	return nil
}
