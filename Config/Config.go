package Config

import (
	"encoding/json"
	"github.com/vmihailenco/redis"
	"io/ioutil"
)

type Redis struct {
	Host     string
	Password string
	Db       int64
}

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

type Config struct {
	Redis   Redis
	Regions []Region
}

func New(fp string) (Config, error) {
	var config Config

	b, err := ioutil.ReadFile(fp)
	if err != nil {
		return config, err
	}

	err = json.Unmarshal(b, &config)
	if err != nil {
		return config, err
	}

	return config, nil
}

func (self Config) GetRedis() (*redis.Client, error) {
	c := redis.NewTCPClient(self.Redis.Host, self.Redis.Password, self.Redis.Db)
	defer c.Close()
	return c, c.Ping().Err()
}
