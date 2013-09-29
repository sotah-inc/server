package main

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Cache/Locale"
	"github.com/ihsw/go-download/Cache/Region"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Log"
	"github.com/ihsw/go-download/Util"
	"github.com/vmihailenco/redis"
	"os"
	"path/filepath"
	// "reflect"
)

func main() {
	Util.Write("Starting...")

	// input validation
	if len(os.Args) == 1 {
		Util.Write("Expected path to config file, got nothing")
		return
	}

	// opening the config file and loading it
	Util.Write("Opening the config file...")
	fp := os.Args[1]
	path, err := filepath.Abs(fp)
	if err != nil {
		Util.Write(err.Error())
		return
	}
	config, err := Config.New(path)
	if err != nil {
		Util.Write(err.Error())
		return
	}

	// connecting the redis clients
	Util.Write("Connecting the redis clients...")
	cache, err := Cache.Connect(config.Redis_Config)
	if err != nil {
		Util.Write(err.Error())
		return
	}

	// flushing all of the databases
	Util.Write("Flushing the databases...")
	var req *redis.StatusReq
	req = cache.Main.FlushDb()
	if req.Err() != nil {
		Util.Write(req.Err().Error())
		return
	}
	for _, c := range cache.Pool {
		req = c.FlushDb()
		if req.Err() != nil {
			Util.Write(req.Err().Error())
			return
		}
	}

	// initializing the locales
	Util.Write("Reading the regions from the config...")
	regions := Region.NewFromList(config.Regions)
	for _, region := range regions {
		for _, locale := range region.Locales {
			locale, err := locale.Persist(cache)
			if err != nil {
				Util.Write(err.Error())
				return
			}
			Util.Write(fmt.Sprintf("Successfully persisted locale %s", locale.Fullname))
			continue
			// marshaling
			s, err := locale.Marshal()

			// persisting

			// retrieving
			derp := []byte(s)
			v := map[string]interface{}{}
			json.Unmarshal(derp, &v)
			l := Locale.Unmarshal(v)
			fmt.Println(l)
		}
	}
	return

	l := Log.New("127.0.0.1:6379", "", 0, "jello")
	l.Write("Jello")

	region := "us"
	Util.Write(fmt.Sprintf("Downloading from %s...", fmt.Sprintf(Status.URL_FORMAT, region)))
	status, err := Status.Get(region)
	if err != nil {
		Util.Write(fmt.Sprintf("GetStatus failed! %s...", err))
		return
	}

	for _, realm := range status.Realms {
		Util.Write(realm.Slug)
	}

	Util.Conclude()
}
