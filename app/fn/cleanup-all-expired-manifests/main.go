package cleanup_all_expired_manifests

import (
	"context"
	"os"

	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state/fn"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	state fn.CleanupAllExpiredManifestsState
)

func init() {
	var err error
	state, err = fn.NewCleanupAllExpiredManifestsState(fn.CleanupAllExpiredManifestsStateConfig{ProjectId: projectId})
	if err != nil {
		logging.Fatalf("Failed to initialize cleanup-all-expired-manifests state: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func CleanupAllExpiredManifests(_ context.Context, _ PubSubMessage) error {
	return state.Run()
}
