package main

import (
	"encoding/json"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
)

type infoResponse struct{}

func newInfoRequest(payload []byte) (infoRequest, error) {
	iRequest := &infoRequest{}
	err := json.Unmarshal(payload, &iRequest)
	if err != nil {
		return infoRequest{}, err
	}

	return *iRequest, nil
}

type infoRequest struct{}

func (iRequest infoRequest) resolve(sta state) infoRequest {
	return infoRequest{}
}

func (sta state) listenForInfo(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.Status, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		iRequest, err := newInfoRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		iResponse := iRequest.resolve(sta)

		encodedStatus, err := json.Marshal(iResponse)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedStatus)
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
