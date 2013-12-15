package Status

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
	"io/ioutil"
	"os"
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
func Get(region Entity.Region, statusDir string, c chan Result) {
	var (
		b        []byte
		response Response
		fileinfo os.FileInfo
	)
	result := Result{
		Response: response,
		Region:   region,
		Error:    nil,
	}

	/*
		checking locally
	*/
	// checking if the file exists
	statusFilepath := fmt.Sprintf("%s/region-%s.json", statusDir, region.Name)
	fileinfo, result.Error = os.Stat(statusFilepath)
	if result.Error != nil && !os.IsNotExist(result.Error) {
		c <- result
		return
	}

	/*
		attempting to load it
		and if it fails *only* due to not being json-decodeable then delete it and continue to the api
	*/
	if fileinfo != nil {
		b, result.Error = ioutil.ReadFile(statusFilepath)
		if result.Error != nil {
			c <- result
			return
		}

		result.Error = json.Unmarshal(b, &response)
		if result.Error == nil {
			result.Response = response
			c <- result
			return
		} else {
			result.Error = os.Remove(statusFilepath)
			if result.Error != nil {
				c <- result
				return
			}
		}
	}

	/*
		falling back to checking the api
	*/
	// downloading from the api
	b, result.Error = Util.Download(fmt.Sprintf(URL_FORMAT, region.Host))
	if result.Error != nil {
		c <- result
		return
	}

	// attempting to json-decode it
	result.Error = json.Unmarshal(b, &response)
	if result.Error != nil {
		c <- result
		return
	}

	// dumping it back to disk
	result.Error = ioutil.WriteFile(statusFilepath, b, 0755)
	if result.Error != nil {
		c <- result
		return
	}

	result.Response = response
	c <- result
}
