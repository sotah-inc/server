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

type RedisConfig struct {
	Main Redis
	Pool []Redis
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
	Redis_Config RedisConfig
	Regions      []Region
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

func GetRedis(r Redis) (*redis.Client, error) {
	c := redis.NewTCPClient(r.Host, r.Password, r.Db)
	defer c.Close()
	return c, c.Ping().Err()
}
