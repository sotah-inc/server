package command

import (
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state/fn"
)

func FnLoadHell(config fn.LoadHellStateConfig) error {
	logging.Info("Starting fn-load-hell")

	// establishing a state
	sta, err := fn.NewLoadHellState(config)
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to establish fn-load-hell")

		return err
	}

	return sta.Run()
}
