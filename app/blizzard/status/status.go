package status

import (
	"encoding/json"
	"fmt"

	"github.com/ihsw/go-download/app/util"
)

/*
	consts
*/
const URL_FORMAT = "https://%s/wow/realm/status?apikey=%s"

/*
	funcs
*/
func Get(region Entity.Region, apiKey string) (response Response, err error) {
	url := fmt.Sprintf(URL_FORMAT, region.Host, apiKey)
	var b []byte
	if b, err = util.Download(url); err != nil {
		return
	}

	if err = json.Unmarshal(b, &response); err != nil {
		return
	}

	return
}

/*
	PvpArea
*/
type PvpArea struct {
	Area                int8
	Controlling_Faction int8
	Status              int8
	Next                int64
}

/*
	Realm
*/
type Realm struct {
	Battlegroup string
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

/*
	Response
*/
type Response struct {
	Realms []Realm
}
