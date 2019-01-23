package internal

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
)

type RegionList []Region

func (rl RegionList) GetPrimaryRegion() (Region, error) {
	for _, reg := range rl {
		if reg.Primary {
			return reg, nil
		}
	}

	return Region{}, errors.New("Could not find primary Region")
}

type RegionName string

func newRegionsFromMessenger(mess messenger.Messenger) (RegionList, error) {
	msg, err := mess.Request(subjects.Regions, []byte{})
	if err != nil {
		return RegionList{}, err
	}

	if msg.Code != codes.Ok {
		return nil, errors.New(msg.Err)
	}

	regs := RegionList{}
	if err := json.Unmarshal([]byte(msg.Data), &regs); err != nil {
		return RegionList{}, err
	}

	return regs, nil
}

type Region struct {
	Name     RegionName `json:"name"`
	Hostname string     `json:"hostname"`
	Primary  bool       `json:"primary"`
}

func (reg Region) GetStatus(res Resolver) (status, error) {
	uri, err := res.AppendAccessToken(res.GetStatusURL(reg.Hostname))
	if err != nil {
		return status{}, err
	}

	stat, _, err := blizzard.NewStatusFromHTTP(uri)
	if err != nil {
		return status{}, err
	}

	return status{stat, reg, newRealms(reg, stat.Realms)}, nil
}

func (reg Region) DatabaseDir(parentDirPath string) string {
	return fmt.Sprintf("%s/%s", parentDirPath, reg.Name)
}
