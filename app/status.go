package app

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const urlFormat = "https://%s/wow/realm/status"

type getStatusURLFunc func(string) string

func defaultGetStatusURL(regionName string) string {
	return fmt.Sprintf(urlFormat, regionName)
}

func newStatus(regionName string, getStatusURL getStatusURLFunc) (*status, error) {
	resp, err := http.Get(getStatusURL(regionName))
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	s := &status{}
	if err := json.Unmarshal(body, s); err != nil {
		return nil, err
	}

	return s, nil
}

type status struct {
	Realms []realm `json:"realms"`
}

type realmSlug string

type realm struct {
	Type            string      `json:"type"`
	Population      string      `json:"population"`
	Queue           bool        `json:"queue"`
	Status          bool        `json:"status"`
	Name            string      `json:"name"`
	Slug            realmSlug   `json:"slug"`
	Battlegroup     string      `json:"battlegroup"`
	Locale          string      `json:"locale"`
	Timezone        string      `json:"timezone"`
	ConnectedRealms []realmSlug `json:"connected_realms"`
}
