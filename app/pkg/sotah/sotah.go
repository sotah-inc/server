package sotah

import "github.com/sotah-inc/server/app/pkg/blizzard"

type Region struct {
	Name     blizzard.RegionName `json:"name"`
	Hostname string              `json:"hostname"`
	Primary  bool                `json:"primary"`
}

type Realms []Realm

type Realm struct {
	blizzard.Realm
	Region       Region
	LastModified int64 `json:"last_modified"`
}
