package Status

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Util"
)

type PvpArea struct {
	Area                uint8
	Controlling_Faction uint8
	Status              int8
	Next                uint64
}

type Realm struct {
	BattleGroup string
	Locale      string
	Name        string
	Population  string
	Queue       bool
	Slug        string
	Status      bool
	Timezone    string
	Tol_Barad   PvpArea
	Type        string
	Wintergrasp PvpArea
}

type Status struct {
	Realms []Realm
}

const UrlFormat = "http://%s.battle.net/api/wow/realm/status"

func Get(region string) (Status, error) {
	var status Status

	b, err := Util.Download(fmt.Sprintf(UrlFormat, region))
	if err != nil {
		return status, err
	}

	err = json.Unmarshal(b, &status)
	if err != nil {
		return status, err
	}

	return status, nil
}
