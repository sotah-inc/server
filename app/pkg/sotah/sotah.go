package sotah

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/util"
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

type RealmTimestamps map[blizzard.RealmSlug]int64

type RegionRealmTimestamps map[blizzard.RegionName]RealmTimestamps

func NormalizeTargetDate(targetDate time.Time) time.Time {
	nearestWeekStartOffset := targetDate.Second() + targetDate.Minute()*60 + targetDate.Hour()*60*60
	return time.Unix(targetDate.Unix()-int64(nearestWeekStartOffset), 0)
}

func NewAuctionManifestFromMap(am map[UnixTimestamp]interface{}) AuctionManifest {
	out := AuctionManifest{}
	for v := range am {
		out = append(out, v)
	}

	return out
}

type AuctionManifest []UnixTimestamp

func (am AuctionManifest) ToMap() map[UnixTimestamp]interface{} {
	out := map[UnixTimestamp]interface{}{}
	for _, v := range am {
		out[v] = struct{}{}
	}

	return out
}

func (am AuctionManifest) EncodeForPersistence() ([]byte, error) {
	jsonEncoded, err := json.Marshal(am)
	if err != nil {
		return []byte{}, err
	}

	return util.GzipEncode(jsonEncoded)
}

func (am AuctionManifest) Includes(subset AuctionManifest) bool {
	amMap := am.ToMap()
	subsetMap := subset.ToMap()
	for subsetTimestamp := range subsetMap {
		if _, ok := amMap[subsetTimestamp]; !ok {
			return false
		}
	}

	return true
}

func (am AuctionManifest) Merge(subset AuctionManifest) AuctionManifest {
	out := am.ToMap()
	subsetMap := subset.ToMap()
	for subsetTimestamp := range subsetMap {
		out[subsetTimestamp] = struct{}{}
	}

	return NewAuctionManifestFromMap(out)
}
