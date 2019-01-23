package state

import (
	"encoding/json"
	"errors"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
)

func newItemClassesFromMessenger(mess messenger.Messenger) (blizzard.ItemClasses, error) {
	msg, err := mess.Request(subjects.ItemClasses, []byte{})
	if err != nil {
		return blizzard.ItemClasses{}, err
	}

	if msg.Code != codes.Ok {
		return blizzard.ItemClasses{}, errors.New(msg.Err)
	}

	return blizzard.NewItemClasses([]byte(msg.Data))
}

func (sta State) ListenForItemClasses(stop ListenStopChan) error {
	err := sta.Messenger.Subscribe(subjects.ItemClasses, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		encodedItemClasses, err := json.Marshal(sta.ItemClasses)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedItemClasses)
		sta.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
