package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	nats "github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func (sta State) NewRegions() (sotah.RegionList, error) {
	msg, err := func() (messenger.Message, error) {
		attempts := 0

		for {
			out, err := sta.IO.Messenger.Request(string(subjects.Boot), []byte{})
			if err == nil {
				return out, nil
			}

			attempts++

			if attempts >= 20 {
				return messenger.Message{}, fmt.Errorf("failed to fetch boot message after %d attempts", attempts)
			}

			logrus.WithField("attempt", attempts).Info("Requested boot, sleeping until next")

			time.Sleep(250 * time.Millisecond)
		}
	}()
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
	Regions     sotah.RegionList     `json:"regions"`
	ItemClasses blizzard.ItemClasses `json:"item_classes"`
	Expansions  []sotah.Expansion    `json:"expansions"`
	Professions []sotah.Profession   `json:"professions"`
}

func (sta APIState) ListenForBoot(stop ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(string(subjects.Boot), stop, func(natsMsg nats.Msg) {
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
