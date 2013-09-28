package main

import (
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Log"
	"github.com/ihsw/go-download/Util"
	"os"
	"path/filepath"
)

func main() {
	Util.Write("Starting...")

	// input validation
	if len(os.Args) == 1 {
		Util.Write("Expected path to config file, got nothing")
		return
	}

	// opening the config file
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

	cache, err := Cache.Connect(config.Redis_Config)
	if err != nil {
		Util.Write(err.Error())
		return
	}
	fmt.Println(cache)
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
