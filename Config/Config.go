package Config

import (
	"encoding/json"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/vmihailenco/redis/v2"
	"io/ioutil"
	"path/filepath"
)

/*
	funcs
*/
func New(source string) (configFile ConfigFile, err error) {
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

// cache intermediates
func NewCacheWrapper(c Connection) (w Cache.Wrapper, err error) {
	r := redis.NewTCPClient(&redis.Options{
		Addr:     c.Host,
		Password: c.Password,
		DB:       c.Db,
	})

	ping := r.Ping()
	if err = ping.Err(); err != nil {
		return
	}

	w = Cache.Wrapper{
		Redis: r,
	}
	return
}

func NewCacheClient(c ConnectionList) (client Cache.Client, err error) {
	client.Main, err = NewCacheWrapper(c.Main)
	if err != nil {
		return
	}

	var w Cache.Wrapper
	for _, poolItem := range c.Pool {
		w, err = NewCacheWrapper(poolItem)
		if err != nil {
			return
		}
		client.Pool = append(client.Pool, w)
	}

	return client, nil
}

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
	Name    string
	Host    string
	Locales []Locale
}

func (self Region) ToEntity() Entity.Region {
	return Entity.Region{
		Name: self.Name,
		Host: self.Host,
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
}
