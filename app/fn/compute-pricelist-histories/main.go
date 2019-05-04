package collectauctions

import (
	"context"
	"os"

	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state/fn"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	sta fn.ComputePricelistHistoriesState
	err error
)

func init() {
	sta, err = fn.NewComputePricelistHistoriesState(fn.ComputePricelistHistoriesStateConfig{ProjectId: projectId})
	if err != nil {
		logging.WithField("error", err.Error()).Fatal("Failed to establish state")

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func ComputePricelistHistories(_ context.Context, m PubSubMessage) error {
	return sta.Run(string(m.Data))
}
