package state

import (
	"encoding/json"
	"errors"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"

	"github.com/sotah-inc/server/app/pkg/messenger"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
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
	RegionName internal.RegionName `json:"region_name"`
}

func (sr StatusRequest) resolve(sta State) (internal.Region, error) {
	var reg internal.Region
	for _, r := range sta.Regions {
		if r.Name != sr.RegionName {
			continue
		}

		reg = r
		break
	}

	if reg.Name == "" {
		return internal.Region{}, errors.New("Invalid region")
	}

	return reg, nil
}

func (sta State) ListenForStatus(stop ListenStopChan) error {
	err := sta.Messenger.Subscribe(subjects.Status, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		sr, err := newStatusRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		reg, err := sr.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.NotFound
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		regionStatus, ok := sta.Statuses[reg.Name]
		if !ok {
			m.Err = "Region found but not in Statuses"
			m.Code = codes.NotFound
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		encodedStatus, err := json.Marshal(regionStatus)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedStatus)
		sta.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta State) NewStatus(reg sotah.Region) (sotah.Status, error) {
	lm := StatusRequest{RegionName: reg.Name}
	encodedMessage, err := json.Marshal(lm)
	if err != nil {
		return sotah.Status{}, err
	}

	msg, err := sta.messenger.Request(subjects.Status, encodedMessage)
	if err != nil {
		return sotah.Status{}, err
	}

	if msg.Code != codes.Ok {
		return sotah.Status{}, errors.New(msg.Err)
	}

	stat, err := blizzard.NewStatus([]byte(msg.Data))
	if err != nil {
		return sotah.Status{}, err
	}

	return sotah.NewStatus(reg, stat), nil
}
