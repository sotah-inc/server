package internal

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/util"
)

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
	ClientID      string                               `json:"client_id"`
	ClientSecret  string                               `json:"client_secret"`
	Regions       RegionList                           `json:"regions"`
	Whitelist     map[RegionName]*getAuctionsWhitelist `json:"whitelist"`
	CacheDir      string                               `json:"cache_dir"`
	UseGCloud     bool                                 `json:"use_gcloud"`
	Expansions    []Expansion                          `json:"expansions"`
	Professions   []Profession                         `json:"professions"`
	ItemBlacklist []blizzard.ItemID                    `json:"item_blacklist"`
}

func (c Config) getRegionWhitelist(rName RegionName) *getAuctionsWhitelist {
	if _, ok := c.Whitelist[rName]; ok {
		return c.Whitelist[rName]
	}

	return nil
}

func (c Config) FilterInRegions(regs RegionList) RegionList {
	out := RegionList{}

	for _, reg := range regs {
		wList, ok := c.Whitelist[reg.Name]
		if ok && wList != nil && len(*wList) == 0 {
			continue
		}

		out = append(out, reg)
	}

	return out
}

func (c Config) filterInRealms(reg Region, reas realms) realms {
	wList, ok := c.Whitelist[reg.Name]
	if !ok {
		return reas
	}
	wListValue := *wList

	out := realms{}

	for _, rea := range reas {
		if _, ok := wListValue[rea.Slug]; !ok {
			continue
		}

		out = append(out, rea)
	}

	return out
}

func (c Config) DatabaseDir() (string, error) {
	return filepath.Abs(fmt.Sprintf("%s/databases", c.CacheDir))
}
