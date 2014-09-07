package Status

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
)

/*
	blizzard json response structs
*/
type PvpArea struct {
	Area                uint8
	Controlling_Faction uint8
	Status              int8
	Next                uint64
}

type Response struct {
	Realms []Realm
}

const URL_FORMAT = "http://%s/api/wow/realm/status"

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

func (self Realm) ToEntity() Entity.Realm {
	return Entity.Realm{
		Name:        self.Name,
		Slug:        self.Slug,
		Battlegroup: self.Battlegroup,
		Type:        self.Type,
		Status:      self.Status,
		Population:  self.Population,
	}
}

/*
	chan structs
*/
type Result struct {
	Response Response
	Region   Entity.Region
	Error    error
}

/*
	funcs
*/
func Get(region Entity.Region, c chan Result) {
	var response Response
	result := Result{
		Response: response,
		Region:   region,
		Error:    nil,
	}

	var b []byte
	b, result.Error = Util.Download(fmt.Sprintf(URL_FORMAT, region.Host))
	if result.Error != nil {
		c <- result
		return
	}

	result.Error = json.Unmarshal(b, &response)
	if result.Error != nil {
		c <- result
		return
	}

	result.Response = response
	c <- result
}
