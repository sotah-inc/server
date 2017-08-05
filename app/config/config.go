package Config

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"

	redis "gopkg.in/redis.v2"
)

/*
	Locale
*/
type Locale struct {
	Name      string
	Fullname  string
	Shortname string
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

/*
	Connection
*/
type Connection struct {
	Host     string
	Password string
	Db       int64
}

func (self Connection) Connect() (r *redis.Client, err error) {
	r = redis.NewTCPClient(&redis.Options{
		Addr:     self.Host,
		Password: self.Password,
		DB:       self.Db,
	})

	ping := r.Ping()
	if err = ping.Err(); err != nil {
		return
	}

	return
}

type ConnectionList struct {
	Main Connection
	Pool []Connection
}

/*
	File
*/
type File struct {
	ConnectionList ConnectionList `json:"redis"`
	Regions        []Region
	ApiKey         string
}

func New(configPath string) (configFile File, err error) {
	var fullConfigPath string
	if fullConfigPath, err = filepath.Abs(configPath); err != nil {
		return
	}

	var b []byte
	if b, err = ioutil.ReadFile(fullConfigPath); err != nil {
		return
	}

	if err = json.Unmarshal(b, &configFile); err != nil {
		return
	}

	return
}
