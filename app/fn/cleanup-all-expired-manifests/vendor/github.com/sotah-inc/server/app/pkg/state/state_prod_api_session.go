package state

import (
	"encoding/json"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/messenger"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func (sta ProdApiState) ListenForSessionSecret(stop ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(string(subjects.SessionSecret), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		encodedData, err := json.Marshal(sessionSecretData{sta.SessionSecret.String()})
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.GenericError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedData)
		sta.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
