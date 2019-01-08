package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sotah-inc/server/app/blizzard"
	"github.com/sotah-inc/server/app/codes"
	"github.com/sotah-inc/server/app/subjects"
)

type regionList []region

func (rl regionList) getPrimaryRegion() (region, error) {
	for _, reg := range rl {
		if reg.Primary {
			return reg, nil
		}
	}

	return region{}, errors.New("Could not find primary region")
}

type regionName string

func newRegionsFromMessenger(mess messenger) (regionList, error) {
	msg, err := mess.request(subjects.Regions, []byte{})
	if err != nil {
		return regionList{}, err
	}

	if msg.Code != codes.Ok {
		return nil, errors.New(msg.Err)
	}

	regs := regionList{}
	if err := json.Unmarshal([]byte(msg.Data), &regs); err != nil {
		return regionList{}, err
	}

	return regs, nil
}

type region struct {
	Name     regionName `json:"name"`
	Hostname string     `json:"hostname"`
	Primary  bool       `json:"primary"`
}

func (reg region) getStatus(res resolver) (status, error) {
	uri, err := res.appendAccessToken(res.getStatusURL(reg.Hostname))
	if err != nil {
		return status{}, err
	}

	stat, _, err := blizzard.NewStatusFromHTTP(uri)
	if err != nil {
		return status{}, err
	}

	return status{stat, reg, newRealms(reg, stat.Realms)}, nil
}

func (reg region) databaseDir(parentDirPath string) string {
	return fmt.Sprintf("%s/%s", parentDirPath, reg.Name)
}
