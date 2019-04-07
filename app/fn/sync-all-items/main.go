package sync_all_items

import (
	"context"
	"encoding/json"
	"os"

	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state/fn"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	state fn.SyncAllItemsState
)

func init() {
	var err error
	state, err = fn.NewSyncAllItemsState(fn.SyncAllItemsStateConfig{ProjectId: projectId})
	if err != nil {
		logging.Fatalf("Failed to initialize sync-all-items-state: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func SyncAllItems(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	logging.Info("Calling state.Run()")

	return state.Run(in)
}
