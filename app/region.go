package main

import (
	"encoding/json"

	"github.com/ihsw/go-download/app/subjects"
)

type regionName string

func newRegionsFromMessenger(mess messenger) ([]*region, error) {
	body, err := mess.request(subjects.Regions, []byte{})
	if err != nil {
		return []*region{}, err
	}

	regs := []*region{}
	if err := json.Unmarshal(body, &regs); err != nil {
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
