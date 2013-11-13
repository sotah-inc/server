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
	RealmType   string `json:"type"`
	Wintergrasp PvpArea
}

type Status struct {
	Realms []Realm
}

type Result struct {
	Status   Status
	RegionId int64
	Error    error
}

const URL_FORMAT = "http://%s/api/wow/realm/status"

func Get(host string, regionId int64, c chan Result) {
	var (
		b      []byte
		err    error
		status Status
	)
	result := Result{
		Status:   status,
		RegionId: regionId,
		Error:    nil,
	}

	b, result.Error = Util.Download(fmt.Sprintf(URL_FORMAT, host))
	if err = result.Error; err != nil {
		c <- result
		return
	}

	err = json.Unmarshal(b, &status)
	if err != nil {
		result.Error = err
		c <- result
		return
	}

	result.Status = status
	c <- result
}
