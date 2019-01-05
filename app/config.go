package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/sotah-inc/server/app/blizzard"
	"github.com/sotah-inc/server/app/logging"
	"github.com/sotah-inc/server/app/util"
)

func newConfigFromFilepath(relativePath string) (config, error) {
	logging.WithField("path", relativePath).Info("Reading config")

	body, err := util.ReadFile(relativePath)
	if err != nil {
		return config{}, err
	}

	return newConfig(body)
}

func newConfig(body []byte) (config, error) {
	c := &config{}
	if err := json.Unmarshal(body, &c); err != nil {
		return config{}, err
	}

	return *c, nil
}

type config struct {
	APIKey           string                               `json:"api_key"`
	Regions          regionList                           `json:"regions"`
	Whitelist        map[regionName]*getAuctionsWhitelist `json:"whitelist"`
	CacheDir         string                               `json:"cache_dir"`
	UseGCloudStorage bool                                 `json:"use_gcloud_storage"`
	Expansions       []expansion                          `json:"expansions"`
	Professions      []profession                         `json:"professions"`
	ItemBlacklist    []blizzard.ItemID                    `json:"item_blacklist"`
}

func (c config) getRegionWhitelist(rName regionName) *getAuctionsWhitelist {
	if _, ok := c.Whitelist[rName]; ok {
		return c.Whitelist[rName]
	}

	return nil
}

func (c config) filterInRegions(regs regionList) regionList {
	out := regionList{}

	for _, reg := range regs {
		wList, ok := c.Whitelist[reg.Name]
		if ok && wList != nil && len(*wList) == 0 {
			continue
		}

		out = append(out, reg)
	}

	return out
}

func (c config) filterInRealms(reg region, reas realms) realms {
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

func (c config) databaseDir() (string, error) {
	return filepath.Abs(fmt.Sprintf("%s/databases", c.CacheDir))
}
