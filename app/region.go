package main

import (
	"encoding/json"
	"errors"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
)

type regionName string

func newRegionsFromMessenger(mess messenger) ([]*region, error) {
	msg, err := mess.request(subjects.Regions, []byte{})
	if err != nil {
		return []*region{}, err
	}

	if msg.Code != codes.Ok {
		return nil, errors.New(msg.Err)
	}

	regs := []*region{}
	if err := json.Unmarshal([]byte(msg.Data), &regs); err != nil {
		return []*region{}, err
	}

	return regs, nil
}

type region struct {
	Name     regionName `json:"name"`
	Hostname string     `json:"hostname"`
}

func (reg region) getStatus(res resolver) (*status, error) {
	return newStatusFromHTTP(reg, res)
}
