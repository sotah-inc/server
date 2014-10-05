package Config

import (
	"encoding/json"
	"github.com/ihsw/go-download/Entity"
	"io/ioutil"
	"path/filepath"
)

/*
	Locale
*/
type Locale struct {
	Name      string
	Fullname  string
	Shortname string
}

func (self Locale) ToEntity() Entity.Locale {
	return Entity.Locale{
		Name:      self.Name,
		Fullname:  self.Fullname,
		Shortname: self.Shortname,
	}
}

/*
	Region
*/
type Region struct {
	Name      string
	Host      string
	Queryable bool
	Locales   []Locale
}

func (self Region) ToEntity() Entity.Region {
	return Entity.Region{
		Name:      self.Name,
		Host:      self.Host,
		Queryable: self.Queryable,
	}
}

/*
	connection info
*/
type Connection struct {
	Host     string
	Password string
	Db       int64
}

type ConnectionList struct {
	Main Connection
	Pool []Connection
}

type ConfigFile struct {
	ConnectionList ConnectionList `json:"redis"`
	Regions        []Region
	ApiKey         string
}

func NewConfigFile(source string) (configFile ConfigFile, err error) {
	var sourceFilepath string
	sourceFilepath, err = filepath.Abs(source)
	if err != nil {
		return
	}
	b, err := ioutil.ReadFile(sourceFilepath)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &configFile)
	if err != nil {
		return
	}

	return
}
