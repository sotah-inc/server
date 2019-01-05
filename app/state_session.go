package main

import (
	"encoding/json"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/codes"
	"github.com/sotah-inc/server/app/subjects"
)

type sessionSecretData struct {
	SessionSecret string `json:"session_secret"`
}

func (sta state) listenForSessionSecret(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.SessionSecret, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		encodedData, err := json.Marshal(sessionSecretData{sta.sessionSecret.String()})
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedData)
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
