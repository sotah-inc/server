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
		reas[i] = Realm{rea, reg, RealmModificationDates{}}
	}

	return reas
}

type Realms []Realm

func NewSkeletonRealm(regionName blizzard.RegionName, realmSlug blizzard.RealmSlug) Realm {
	return Realm{
		Region: Region{Name: regionName},
		Realm:  blizzard.Realm{Slug: realmSlug},
	}
}

type RealmModificationDates struct {
	Downloaded                 int64 `json:"downloaded"`
	LiveAuctionsReceived       int64 `json:"live_auctions_received"`
	PricelistHistoriesReceived int64 `json:"pricelist_histories_received"`
}

type Realm struct {
	blizzard.Realm
	Region                 Region                 `json:"region"`
	RealmModificationDates RealmModificationDates `json:"realm_modification_dates"`
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

type RegionRealms map[blizzard.RegionName]Realms

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
	for _, subsetTimestamp := range subset {
		out[subsetTimestamp] = struct{}{}
	}

	return NewAuctionManifestFromMap(out)
}

func NewBlizzardCredentials(data []byte) (BlizzardCredentials, error) {
	var out BlizzardCredentials
	if err := json.Unmarshal(data, &out); err != nil {
		return BlizzardCredentials{}, err
	}

	return out, nil
}

type BlizzardCredentials struct {
	ClientId     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

func (c BlizzardCredentials) EncodeForStorage() ([]byte, error) {
	return json.Marshal(c)
}

type PricelistHistoryVersions map[blizzard.RegionName]map[blizzard.RealmSlug]map[UnixTimestamp]string

func (v PricelistHistoryVersions) Insert(
	regionName blizzard.RegionName,
	realmSlug blizzard.RealmSlug,
	targetTimestamp UnixTimestamp,
	version string,
) PricelistHistoryVersions {
	if _, ok := v[regionName]; !ok {
		v[regionName] = map[blizzard.RealmSlug]map[UnixTimestamp]string{}
	}
	if _, ok := v[regionName][realmSlug]; !ok {
		v[regionName][realmSlug] = map[UnixTimestamp]string{}
	}

	v[regionName][realmSlug][targetTimestamp] = version

	return v
}

func NewItemIdsBatches(ids blizzard.ItemIds, batchSize int) ItemIdBatches {
	batches := ItemIdBatches{}
	for i, id := range ids {
		key := (i - (i % batchSize)) / batchSize
		batch := func() blizzard.ItemIds {
			out, ok := batches[key]
			if !ok {
				return blizzard.ItemIds{}
			}

			return out
		}()
		batch = append(batch, id)

		batches[key] = batch
	}

	return batches
}

type ItemIdBatches map[int]blizzard.ItemIds

func NewIconItemsPayloadsBatches(iconIdsMap map[string]blizzard.ItemIds, batchSize int) IconItemsPayloadsBatches {
	batches := IconItemsPayloadsBatches{}
	i := 0
	for iconName, itemIds := range iconIdsMap {
		key := (i - (i % batchSize)) / batchSize
		batch := func() IconItemsPayloads {
			out, ok := batches[key]
			if !ok {
				return IconItemsPayloads{}
			}

			return out
		}()
		batch = append(batch, IconItemsPayload{Name: iconName, Ids: itemIds})

		batches[key] = batch

		i += 1
	}

	return batches
}

type IconItemsPayloadsBatches map[int]IconItemsPayloads

func NewIconItemsPayloads(data string) (IconItemsPayloads, error) {
	var out IconItemsPayloads
	if err := json.Unmarshal([]byte(data), &out); err != nil {
		return IconItemsPayloads{}, err
	}

	return out, nil
}

type IconItemsPayloads []IconItemsPayload

func (d IconItemsPayloads) EncodeForDelivery() (string, error) {
	jsonEncoded, err := json.Marshal(d)
	if err != nil {
		return "", err
	}

	return string(jsonEncoded), nil
}

type IconItemsPayload struct {
	Name string
	Ids  blizzard.ItemIds
}
