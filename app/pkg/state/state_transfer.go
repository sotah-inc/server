package state

import (
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
	transferState.InStoreClient = inStoreClient

	outStoreClient, err := store.NewClient(config.OutProjectId)
	if err != nil {
		return TransferState{}, err
	}
	transferState.outStoreClient = outStoreClient

	return transferState, nil
}

type TransferState struct {
	State

	InStoreClient  store.Client
	outStoreClient store.Client
}

func (transferState TransferState) Run() error {
	return nil
}
