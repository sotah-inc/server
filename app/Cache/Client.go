package Cache

import (
	"github.com/ihsw/go-download/app/Config"
	redis "gopkg.in/redis.v2"
)

/*
	funcs
*/
func NewClient(configFile Config.File) (client Client, err error) {
	client.ApiKey = configFile.ApiKey
	if client.Main, err = NewWrapper(configFile.ConnectionList.Main); err != nil {
		return
	}

	for _, c := range configFile.ConnectionList.Pool {
		var w Wrapper
		if w, err = NewWrapper(c); err != nil {
			return
		}

		client.Pool = append(client.Pool, w)
	}

	return client, nil
}

/*
	Client
*/
type Client struct {
	Main   Wrapper
	Pool   []Wrapper
	ApiKey string
}

func (self Client) FlushDb() (err error) {
	var (
		cmd *redis.StatusCmd
	)
	cmd = self.Main.Redis.FlushDb()
	if err = cmd.Err(); err != nil {
		return
	}
	for _, w := range self.Pool {
		cmd = w.Redis.FlushDb()
		if err = cmd.Err(); err != nil {
			return
		}
	}

	return nil
}
