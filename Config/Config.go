package Config

import (
	"errors"
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

// reading in os.args
func Initialize(args []string) (ConfigFile, Cache.Client, error) {
	var (
		configFile ConfigFile
		client     Cache.Client
		err        error
	)

	if len(args) == 1 {
		err = errors.New("Expected path to config file, got nothing")
		return configFile, client, err
	}

	// loading the config-file
	configFile, err = New(args[1])
	if err != nil {
		return configFile, client, err
	}

	// connecting the redis clients
	client, err = NewCacheClient(configFile.ConnectionList)
	if err != nil {
		return configFile, client, err
	}

	// flushing all of the databases
	err = client.FlushDb()
	if err != nil {
		return configFile, client, err
	}

	return configFile, client, nil
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
}
