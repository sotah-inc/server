package main

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Log"
	"github.com/ihsw/go-download/Util"
	"os"
	"path/filepath"
)

func main() {
	Util.Write("Starting...")

	var (
		err    error
		client Cache.Client
		config Config.Config
	)

	/*
		initialization
	*/
	if len(os.Args) == 1 {
		Util.Write("Expected path to config file, got nothing")
		return
	}
	config, err = Config.NewConfig(os.Args[1])
	if err != nil {
		Util.Write(err.Error())
		return
	}
	return

	/*
		reading the config
	*/
	// managers
	regionManager := Region.Manager{Client: client}
	localeManager := Locale.Manager{Client: client}

	// initializing the locales
	Util.Write("Reading the regions from the config...")
	regions := Region.NewFromList(config.Regions)
	for _, region := range regions {
		for _, locale := range region.Locales {
			locale, err := localeManager.Persist(locale)
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
