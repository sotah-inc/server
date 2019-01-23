package state

import (
	"encoding/json"
	"errors"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/codes"
	"github.com/sotah-inc/server/app/subjects"
)

func newRuntimeInfoDataFromMessenger(mess messenger) (runtimeInfoData, error) {
	msg, err := mess.request(subjects.RuntimeInfo, []byte{})
	if err != nil {
		return runtimeInfoData{}, err
	}

	if msg.Code != codes.Ok {
		return runtimeInfoData{}, errors.New(msg.Err)
	}

	return newRuntimeInfoData([]byte(msg.Data))
}

func newRuntimeInfoData(data []byte) (runtimeInfoData, error) {
	out := &runtimeInfoData{}
	if err := json.Unmarshal(data, out); err != nil {
		return runtimeInfoData{}, err
	}

	return *out, nil
}

type runtimeInfoData struct {
	runID string `json:"run_id"`
}

func (sta State) listenForRuntimeInfo(stop ListenStopChan) error {
	err := sta.Messenger.subscribe(subjects.RuntimeInfo, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		out := runtimeInfoData{
			runID: sta.runID.String(),
		}

		encodedData, err := json.Marshal(out)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedData)
		sta.Messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
