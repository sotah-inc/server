package app

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const statusURLFormat = "https://%s/wow/realm/status"

type getStatusURLFunc func(string) string

func defaultGetStatusURL(regionHostname string) string {
	return fmt.Sprintf(statusURLFormat, regionHostname)
}

func newStatus(regionHostname string, r resolver) (*status, error) {
	resp, err := http.Get(r.getStatusURL(regionHostname))
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
	Realms realms `json:"realms"`
}
