package blizzard

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sotah-inc/server/app/metric"

	"github.com/sotah-inc/server/app/blizzard/realmpopulations"
	"github.com/sotah-inc/server/app/blizzard/realmtypes"
	"github.com/sotah-inc/server/app/util"
)

// NewRealmFromFilepath loads a realm from a json file
func NewRealmFromFilepath(relativeFilepath string) (Realm, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return Realm{}, err
	}

	return newRealm(body)
}

func newRealm(body []byte) (Realm, error) {
	rea := &Realm{}
	if err := json.Unmarshal(body, &rea); err != nil {
		return Realm{}, err
	}

	return *rea, nil
}

// RealmSlug is the region-specific unique identifier
type RealmSlug string

// Realm represents a given realm
type Realm struct {
	Type            realmtypes.RealmType             `json:"type"`
	Population      realmpopulations.RealmPopulation `json:"population"`
	Queue           bool                             `json:"queue"`
	Status          bool                             `json:"status"`
	Name            string                           `json:"name"`
	Slug            RealmSlug                        `json:"slug"`
	Battlegroup     string                           `json:"battlegroup"`
	Locale          string                           `json:"locale"`
	Timezone        string                           `json:"timezone"`
	ConnectedRealms []RealmSlug                      `json:"connected_realms"`
}

const statusURLFormat = "https://%s/wow/realm/status?locale=en_US"

// GetStatusURLFunc defines the expected func signature for generating a status uri
type GetStatusURLFunc func(string) string

// DefaultGetStatusURL returns a formatted uri
func DefaultGetStatusURL(regionHostname string) string {
	return fmt.Sprintf(statusURLFormat, regionHostname)
}

// NewStatusFromHTTP loads a status from a uri
func NewStatusFromHTTP(uri string) (Status, ResponseMeta, error) {
	resp, err := Download(uri)
	if err != nil {
		return Status{}, ResponseMeta{}, err
	}

	if resp.Status != 200 {
		return Status{}, ResponseMeta{}, errors.New("Status was not 200")
	}

	status, err := NewStatus(resp.Body)
	if err != nil {
		return Status{}, ResponseMeta{}, err
	}

	metric.ReportBlizzardAPIIngress("blizzard.NewStatusFromHTTP()", resp.ContentLength)

	return status, resp, nil
}

// NewStatusFromFilepath loads a status from a json file
func NewStatusFromFilepath(relativeFilepath string) (Status, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return Status{}, err
	}

	return NewStatus(body)
}

// NewStatus loads a status from a byte array of json
func NewStatus(body []byte) (Status, error) {
	s := &Status{}
	if err := json.Unmarshal(body, s); err != nil {
		return Status{}, err
	}

	return *s, nil
}

// Status contains a list of realms
type Status struct {
	Realms []Realm `json:"realms"`
}
