package Config

import (
	"encoding/json"
	"github.com/ihsw/go-download/Cache"
	"io/ioutil"
	"path/filepath"
)

/*
	misc
*/
type Locale struct {
	Name      string
	Fullname  string
	Shortname string
}

type Region struct {
	Name    string
	Host    string
	Locales []Locale
}

/*
	Redis
*/
type Redis struct {
	host     string
	password string
	db       int64
}

func (self Redis) Host() string {
	return self.host
}

func (self Redis) Password() string {
	return self.password
}

func (self Redis) Db() int64 {
	return self.db
}

/*
	RedisConfig
*/
type RedisConfig struct {
	main Redis
	pool []Redis
}

func (self RedisConfig) Main() Redis {
	return self.main
}

func (self RedisConfig) Pool() []Redis {
	return self.pool
}

/*
	Config
*/
type Config struct {
	Redis_Config RedisConfig
	Regions      []Region
}

func (self Config) New(source string) (config Config, err error) {
	var (
		sourceFilepath string
		client         Cache.Client
	)

	// opening the config file and loading it
	sourceFilepath, err = filepath.Abs(source)
	if err != nil {
		return
	}
	b, err := ioutil.ReadFile(sourceFilepath)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &config)
	if err != nil {
		return
	}

	// connecting the redis clients
	client, err = Cache.NewClient(config.Redis_Config)
	if err != nil {
		return
	}

	// flushing all of the databases
	err = client.FlushAll()
	if err != nil {
		return
	}

	return
}
