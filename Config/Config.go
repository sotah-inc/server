package Config

import (
	"encoding/json"
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
	Host     string
	Password string
	Db       int64
}

/*
	RedisConfig
*/
type RedisConfig struct {
	Main Redis
	Pool []Redis
}

/*
	Config
*/
type Config struct {
	Redis_Config RedisConfig
	Regions      []Region
}

func New(source string) (config Config, err error) {
	var sourceFilepath string
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

	return
}
