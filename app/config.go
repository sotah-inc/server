package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/util"
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

func (c config) databaseDir() (string, error) {
	return filepath.Abs(fmt.Sprintf("%s/databases", c.CacheDir))
}
