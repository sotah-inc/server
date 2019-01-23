package state

import (
	"encoding/json"
	"errors"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/codes"
	"github.com/sotah-inc/server/app/subjects"
)

func newStatusRequest(payload []byte) (StatusRequest, error) {
	sr := &StatusRequest{}
	err := json.Unmarshal(payload, &sr)
	if err != nil {
		return StatusRequest{}, err
	}

	return *sr, nil
}

type StatusRequest struct {
	RegionName regionName `json:"region_name"`
}

func (sr StatusRequest) resolve(sta State) (region, error) {
	var reg region
	for _, r := range sta.regions {
		if r.Name != sr.RegionName {
			continue
		}

		reg = r
		break
	}

	if reg.Name == "" {
		return region{}, errors.New("Invalid region")
	}

	return reg, nil
}

func (sta State) listenForStatus(stop listenStopChan) error {
	err := sta.Messenger.subscribe(subjects.Status, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		sr, err := newStatusRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		reg, err := sr.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.NotFound
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		regionStatus, ok := sta.statuses[reg.Name]
		if !ok {
			m.Err = "Region found but not in statuses"
			m.Code = codes.NotFound
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		encodedStatus, err := json.Marshal(regionStatus)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedStatus)
		sta.Messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
