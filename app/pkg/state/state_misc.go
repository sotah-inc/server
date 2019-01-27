package state

import (
	"encoding/json"
	"errors"

	"github.com/sotah-inc/server/app/pkg/sotah"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
)

func (sta State) ListenForRegions(stop messenger.ListenStopChan) error {
	err := sta.IO.messenger.Subscribe(subjects.Regions, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		encodedRegions, err := json.Marshal(sta.Regions)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedRegions)
		sta.IO.messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta State) NewRegions() (sotah.RegionList, error) {
	msg, err := sta.IO.messenger.Request(subjects.Regions, []byte{})
	if err != nil {
		return sotah.RegionList{}, err
	}

	if msg.Code != codes.Ok {
		return nil, errors.New(msg.Err)
	}

	regs := sotah.RegionList{}
	if err := json.Unmarshal([]byte(msg.Data), &regs); err != nil {
		return sotah.RegionList{}, err
	}

	return regs, nil
}

type bootResponse struct {
	Regions     internal.RegionList   `json:"Regions"`
	ItemClasses blizzard.ItemClasses  `json:"item_classes"`
	Expansions  []internal.Expansion  `json:"expansions"`
	Professions []internal.Profession `json:"professions"`
}

func (sta State) ListenForBoot(stop messenger.ListenStopChan) error {
	err := sta.IO.messenger.Subscribe(subjects.Boot, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		encodedResponse, err := json.Marshal(bootResponse{
			Regions:     sta.Regions,
			ItemClasses: sta.ItemClasses,
			Expansions:  sta.expansions,
			Professions: sta.professions,
		})
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedResponse)
		sta.IO.messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta State) ListenForGenericTestErrors(stop messenger.ListenStopChan) error {
	err := sta.IO.messenger.Subscribe(subjects.GenericTestErrors, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()
		m.Err = "Test error"
		m.Code = codes.GenericError
		sta.IO.messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
