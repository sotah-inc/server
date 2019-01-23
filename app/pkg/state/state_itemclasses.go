package state

import (
	"encoding/json"
	"errors"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/blizzard"
	"github.com/sotah-inc/server/app/codes"
	"github.com/sotah-inc/server/app/subjects"
)

func newItemClassesFromMessenger(mess messenger) (blizzard.ItemClasses, error) {
	msg, err := mess.request(subjects.ItemClasses, []byte{})
	if err != nil {
		return blizzard.ItemClasses{}, err
	}

	if msg.Code != codes.Ok {
		return blizzard.ItemClasses{}, errors.New(msg.Err)
	}

	return blizzard.NewItemClasses([]byte(msg.Data))
}

func (sta State) listenForItemClasses(stop ListenStopChan) error {
	err := sta.Messenger.subscribe(subjects.ItemClasses, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		encodedItemClasses, err := json.Marshal(sta.itemClasses)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedItemClasses)
		sta.Messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
