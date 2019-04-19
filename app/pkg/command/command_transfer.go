package command

import (
	"time"

	"github.com/sirupsen/logrus"
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
	startTime := time.Now()
	if err := transferState.Run(); err != nil {
		logging.WithFields(logrus.Fields{
			"error":          err.Error(),
			"duration-in-ms": int64(time.Now().Sub(startTime)) / 1000 / 1000,
		}).Error("Failed to run")

		return err
	}

	logging.WithFields(logrus.Fields{
		"duration-in-ms": int64(time.Now().Sub(startTime)) / 1000 / 1000,
	}).Info("Exiting")
	return nil
}
