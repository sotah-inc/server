package state

import (
	"encoding/json"

	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

type sessionSecretData struct {
	SessionSecret string `json:"session_secret"`
}

func (sta APIState) ListenForSessionSecret(stop ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(string(subjects.SessionSecret), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		encodedData, err := json.Marshal(sessionSecretData{sta.SessionSecret.String()})
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
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
