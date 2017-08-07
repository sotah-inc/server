package app

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const urlFormat = "https://%s/wow/realm/status"

func newStatus(regionName string) (*status, error) {
	return getStatus(fmt.Sprintf(urlFormat, regionName))
}

func getStatus(url string) (*status, error) {
	resp, err := http.Get(url)
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

func main() {
	fmt.Printf("Hello, world!")
}
