package main

import (
	"encoding/json"

	"github.com/ihsw/go-download/app/util"
)

func newConfigFromFilepath(relativePath string) (*config, error) {
	body, err := util.ReadFile(relativePath)
	if err != nil {
		return nil, err
	}

	return newConfig(body)
}

func newConfig(body []byte) (*config, error) {
	c := &config{}
	if err := json.Unmarshal(body, &c); err != nil {
		return nil, err
	}

	return c, nil
}

type config struct {
	APIKey    string    `json:"api_key"`
	Regions   []region  `json:"regions"`
	Whitelist whitelist `json:"whitelist"`
}

type whitelist map[regionName]getAuctionsWhitelist

