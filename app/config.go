package main

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
)

func newConfig(relativePath string) (*config, error) {
	path, err := filepath.Abs(relativePath)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	c := &config{}
	if err = json.Unmarshal(body, &c); err != nil {
		return nil, err
	}

	return c, nil
}

type config struct {
	APIKey  string   `json:"api_key"`
	Regions []region `json:"regions"`
}

type regionName string

type region struct {
	Name     regionName `json:"name"`
	Hostname string     `json:"hostname"`
}

func (reg region) getStatus(res resolver) (*status, error) {
	return newStatusFromHTTP(reg, res)
}
