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
	Area                int8
	Controlling_Faction int8
	Status              int8
	Next                int64
}

type Response struct {
	Realms []Realm
}

const URL_FORMAT = "https://%s/wow/realm/status?apikey=%s"

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
func Get(region Entity.Region, apiKey string, c chan Result) {
	result := Result{
		Response: Response{},
		Region:   region,
		Error:    nil,
	}

	var b []byte
	if b, result.Error = Util.Download(fmt.Sprintf(URL_FORMAT, region.Host, apiKey)); result.Error != nil {
		c <- result
		return
	}

	if result.Error = json.Unmarshal(b, &result.Response); result.Error != nil {
		c <- result
		return
	}

	c <- result
}
