package main

import (
	"fmt"
	"github.com/ihsw/go-download/util"
)

var status_url = "http://us.battle.net/api/wow/realm/status"

func main() {
	util.Write("Start")

	contents, err := util.Download(status_url)
	if err != nil {
		util.Write(fmt.Sprintf("Failed! %s", err))
		return
	}

	for k, _ := range contents {
		util.Write(k)
	}

	util.Write("Success")
}
