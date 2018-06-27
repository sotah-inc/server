package main

import (
	"errors"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
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
