package main

import (
	"encoding/json"
	"fmt"

	"github.com/ihsw/sotah-server/app/subjects"

	"github.com/ihsw/sotah-server/app/util"
)

const statusURLFormat = "https://%s/wow/realm/status?locale=en_US"

type getStatusURLFunc func(string) string

func defaultGetStatusURL(regionHostname string) string {
	return fmt.Sprintf(statusURLFormat, regionHostname)
}

func newStatusFromHTTP(reg region, r resolver) (*status, error) {
	body, err := r.get(r.getStatusURL(reg.Hostname))
	if err != nil {
		return nil, err
	}

	return newStatus(reg, body)
}

func newStatusFromMessenger(reg region, mess messenger) (*status, error) {
	lm := listenForStatusMessage{RegionName: reg.Name}
	encodedMessage, err := json.Marshal(lm)
	if err != nil {
		return nil, err
	}

	data, err := mess.request(subjects.Status, encodedMessage)
	if err != nil {
		return nil, err
	}

	return newStatus(reg, data)
}

func newStatusFromFilepath(reg region, relativeFilepath string) (*status, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return nil, err
	}

	return newStatus(reg, body)
}

func newStatus(reg region, body []byte) (*status, error) {
	s := &status{region: reg}
	if err := json.Unmarshal(body, s); err != nil {
		return nil, err
	}

	for i, realm := range s.Realms {
		realm.region = reg
		s.Realms[i] = realm
	}

	return s, nil
}

type status struct {
	Realms realms `json:"realms"`

	region region
}
