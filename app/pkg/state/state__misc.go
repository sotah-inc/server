package state

import (
	"encoding/json"
	"errors"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func (sta State) NewRegions() (sotah.RegionList, error) {
	msg, err := sta.IO.Messenger.Request(subjects.Boot, []byte{})
	if err != nil {
		return sotah.RegionList{}, err
	}

	if msg.Code != codes.Ok {
		return nil, errors.New(msg.Err)
	}

	boot := bootResponse{}
	if err := json.Unmarshal([]byte(msg.Data), &boot); err != nil {
		return sotah.RegionList{}, err
	}

	return boot.Regions, nil
}

type bootResponse struct {
	Regions     sotah.RegionList     `json:"Regions"`
	ItemClasses blizzard.ItemClasses `json:"item_classes"`
	Expansions  []sotah.Expansion    `json:"expansions"`
	Professions []sotah.Profession   `json:"professions"`
}

func (sta APIState) ListenForBoot(stop messenger.ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(subjects.Boot, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		encodedResponse, err := json.Marshal(bootResponse{
			Regions:     sta.Regions,
			ItemClasses: sta.ItemClasses,
			Expansions:  sta.Expansions,
			Professions: sta.Professions,
		})
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedResponse)
		sta.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta State) ListenForGenericTestErrors(stop messenger.ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(subjects.GenericTestErrors, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()
		m.Err = "Test error"
		m.Code = codes.GenericError
		sta.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
