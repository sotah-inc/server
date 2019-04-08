package sotah

import (
	"encoding/json"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/util"
)

type realmWhitelist map[blizzard.RealmSlug]struct{}

func NewConfigFromFilepath(relativePath string) (Config, error) {
	logging.WithField("path", relativePath).Info("Reading Config")

	body, err := util.ReadFile(relativePath)
	if err != nil {
		return Config{}, err
	}

	return newConfig(body)
}

func newConfig(body []byte) (Config, error) {
	c := &Config{}
	if err := json.Unmarshal(body, &c); err != nil {
		return Config{}, err
	}

	return *c, nil
}

type Config struct {
	Regions       RegionList                                   `json:"regions"`
	Whitelist     map[blizzard.RegionName][]blizzard.RealmSlug `json:"whitelist"`
	UseGCloud     bool                                         `json:"use_gcloud"`
	Expansions    []Expansion                                  `json:"expansions"`
	Professions   []Profession                                 `json:"professions"`
	ItemBlacklist []blizzard.ItemID                            `json:"item_blacklist"`
}

func (c Config) FilterInRegions(regs RegionList) RegionList {
	out := RegionList{}

	for _, reg := range regs {
		if _, ok := c.Whitelist[reg.Name]; !ok {
			continue
		}

		out = append(out, reg)
	}

	return out
}

func (c Config) FilterInRealms(reg Region, reas Realms) Realms {
	// returning nothing when region not found in whitelist
	wList, ok := c.Whitelist[reg.Name]
	if !ok {
		return Realms{}
	}

	// returning all when whitelist is empty
	if len(wList) == 0 {
		return reas
	}

	// gathering flags
	wListValue := realmWhitelist{}
	for _, realmSlug := range wList {
		wListValue[realmSlug] = struct{}{}
	}

	out := Realms{}

	for _, rea := range reas {
		if _, ok := wListValue[rea.Slug]; !ok {
			continue
		}

		out = append(out, rea)
	}

	return out
}
