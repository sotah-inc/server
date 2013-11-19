package Status

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
)

/*
	misc
*/
type PvpArea struct {
	Area                uint8
	Controlling_Faction uint8
	Status              int8
	Next                uint64
}

type Status struct {
	Realms []Realm
}

type Result struct {
	Status Status
	Region Entity.Region
	Error  error
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

const URL_FORMAT = "http://%s/api/wow/realm/status"

func Get(region Entity.Region, c chan Result) {
	var (
		b      []byte
		err    error
		status Status
	)
	result := Result{
		Status: status,
		Region: region,
		Error:  nil,
	}

	b, result.Error = Util.Download(fmt.Sprintf(URL_FORMAT, region.Host))
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
