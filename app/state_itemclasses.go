package main

import (
	"encoding/json"
	"errors"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
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

func (sta state) listenForItemClasses(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.ItemClasses, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		encodedItemClasses, err := json.Marshal(sta.itemClasses)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedItemClasses)
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
