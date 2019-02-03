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

	return Region{}, errors.New("could not find primary region")
}

func (rl RegionList) GetRegion(name blizzard.RegionName) Region {
	for _, reg := range rl {
		if reg.Name == name {
			return reg
		}
	}

	return Region{}
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
	Region       Region `json:"region"`
	LastModified int64  `json:"last_modified"`
}

func NewStatus(reg Region, stat blizzard.Status) Status {
	return Status{stat, reg, NewRealms(reg, stat.Realms)}
}

type Status struct {
	blizzard.Status
	Region Region `json:"-"`
	Realms Realms `json:"realms"`
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

type RegionRealmMap map[blizzard.RegionName]RealmMap

type RealmMap map[blizzard.RealmSlug]Realm

func (rMap RealmMap) ToRealms() Realms {
	out := Realms{}
	for _, realm := range rMap {
		out = append(out, realm)
	}

	return out
}

type UnixTimestamp int64

type WorkerStopChan chan struct{}
