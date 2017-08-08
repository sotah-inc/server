package app

import (
	"encoding/json"
	"fmt"

	"github.com/ihsw/go-download/app/util"
)

const statusURLFormat = "https://%s/wow/realm/status"

type getStatusURLFunc func(string) string

func defaultGetStatusURL(regionHostname string) string {
	return fmt.Sprintf(statusURLFormat, regionHostname)
}

func newStatus(reg region, r resolver) (*status, error) {
	body, err := util.Download(r.getStatusURL(reg.Hostname))
	if err != nil {
		return nil, err
	}

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
