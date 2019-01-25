package sotah

import (
	"errors"
	"github.com/sotah-inc/server/app/pkg/blizzard"
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

type Region struct {
	Name     blizzard.RegionName `json:"name"`
	Hostname string              `json:"hostname"`
	Primary  bool                `json:"primary"`
}

func NewRealms(reg Region, blizzRealms []blizzard.Realm) Realms {
	reas := make([]Realm, len(blizzRealms))
	for i, rea := range blizzRealms {
		reas[i] = Realm{rea, reg, 0}
	}

	return reas
}

type Realms []Realm

type Realm struct {
	blizzard.Realm
	Region       Region
	LastModified int64 `json:"last_modified"`
}

func NewStatus(reg Region, stat blizzard.Status) Status {
	return Status{stat, reg, NewRealms(reg, stat.Realms)}
}

type Status struct {
	blizzard.Status
	Region Region
	Realms Realms `json:"Realms"`
}

type Statuses map[blizzard.RegionName]Status

type Profession struct {
	Name    string `json:"name"`
	Label   string `json:"label"`
	Icon    string `json:"icon"`
	IconURL string `json:"icon_url"`
}

type Expansion struct {
	Name       string `json:"name"`
	Label      string `json:"label"`
	Primary    bool   `json:"primary"`
	LabelColor string `json:"label_color"`
}