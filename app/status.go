package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ihsw/sotah-server/app/codes"

	"github.com/ihsw/sotah-server/app/subjects"

	"github.com/ihsw/sotah-server/app/util"
)

const statusURLFormat = "https://%s/wow/realm/status?locale=en_US"

type getStatusURLFunc func(string) string

func defaultGetStatusURL(regionHostname string) string {
	return fmt.Sprintf(statusURLFormat, regionHostname)
}

func newStatusFromHTTP(reg region, r resolver) (*Status, error) {
	body, err := r.get(r.getStatusURL(reg.Hostname))
	if err != nil {
		return nil, err
	}

	return newStatus(reg, body)
}

func newStatusFromMessenger(reg region, mess messenger) (*Status, error) {
	lm := listenForStatusMessage{RegionName: reg.Name}
	encodedMessage, err := json.Marshal(lm)
	if err != nil {
		return nil, err
	}

	msg, err := mess.request(subjects.Status, encodedMessage)
	if err != nil {
		return nil, err
	}

	if msg.Code != codes.Ok {
		return nil, errors.New(msg.Err)
	}

	return newStatus(reg, []byte(msg.Data))
}

func NewStatusFromFilepath(reg region, relativeFilepath string) (*Status, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return nil, err
	}

	return newStatus(reg, body)
}

func newStatus(reg region, body []byte) (*Status, error) {
	s := &Status{region: reg}
	if err := json.Unmarshal(body, s); err != nil {
		return nil, err
	}

	for i, realm := range s.Realms {
		realm.region = reg
		s.Realms[i] = realm
	}

	return s, nil
}

type Status struct {
	Realms realms `json:"realms"`

	region region
}
