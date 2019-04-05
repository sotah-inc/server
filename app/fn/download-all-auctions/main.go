package download_all_auctions

import (
	"context"
	"log"
	"os"

	"github.com/sotah-inc/server/app/pkg/state/fn"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	state fn.DownloadAllAuctionsState
)

func init() {
	var err error
	state, err = fn.NewDownloadAllAuctionsState(fn.DownloadAllAuctionsStateConfig{ProjectId: projectId})
	if err != nil {
		log.Fatalf("Failed to initialize state: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func DownloadAllAuctions(_ context.Context, _ PubSubMessage) error {
	return state.Run()
}
