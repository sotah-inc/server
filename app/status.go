package main

import (
	"encoding/json"
	"errors"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
)

func newStatusFromMessenger(reg region, mess messenger) (status, error) {
	lm := statusRequest{RegionName: reg.Name}
	encodedMessage, err := json.Marshal(lm)
	if err != nil {
		return status{}, err
	}

	msg, err := mess.request(subjects.Status, encodedMessage)
	if err != nil {
		return status{}, err
	}

	if msg.Code != codes.Ok {
		return status{}, errors.New(msg.Err)
	}

	stat, err := blizzard.NewStatus([]byte(msg.Data))
	if err != nil {
		return status{}, err
	}

	return newStatus(reg, stat), nil
}

func newStatusFromFilepath(reg region, relativeFilepath string) (status, error) {
	stat, err := blizzard.NewStatusFromFilepath(relativeFilepath)
	if err != nil {
		return status{}, err
	}

	return newStatus(reg, stat), nil
}

func newStatus(reg region, stat blizzard.Status) status {
	return status{stat, reg, newRealms(reg, stat.Realms)}
}

type status struct {
	blizzard.Status
	region region
	Realms realms
}
