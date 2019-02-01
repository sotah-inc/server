package state

import (
	"encoding/json"
	"errors"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
)

func newRuntimeInfoDataFromMessenger(mess messenger.Messenger) (runtimeInfoData, error) {
	msg, err := mess.Request(subjects.RuntimeInfo, []byte{})
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

func (sta State) ListenForRuntimeInfo(stop messenger.ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(subjects.RuntimeInfo, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		out := runtimeInfoData{
			runID: sta.RunID.String(),
		}

		encodedData, err := json.Marshal(out)
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
