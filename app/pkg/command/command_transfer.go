package command

import (
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state"
)

func Transfer(config state.TransferStateConfig) error {
	logging.Info("Starting transfer")

	// establishing a state
	transferState, err := state.NewTransferState(config)
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to establish transfer-state")

		return err
	}

	// running it
	if err := transferState.Run(); err != nil {
		logging.WithField("error", err.Error()).Error("Failed to run")

		return err
	}

	logging.Info("Exiting")
	return nil
}
