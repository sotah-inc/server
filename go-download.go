package main

import (
	"fmt"
	"github.com/ihsw/go-download/Blizzard"
	"github.com/ihsw/go-download/log"
	"github.com/ihsw/go-download/util"
)

func main() {
	util.Write("Starting...")

	l := log.New("127.0.0.1:6379", "", 0, "jello")
	l.Write("Jello")

	region := "us"
	util.Write(fmt.Sprintf("Downloading from %s...", fmt.Sprintf(Blizzard.StatusUrlFormat, region)))
	status, err := Blizzard.GetStatus(region)
	if err != nil {
		util.Write(fmt.Sprintf("GetStatus failed! %s...", err))
		return
	}

	for _, realm := range status.Realms {
		util.Write(realm.Slug)
	}

	util.Conclude()
}
