package main

import (
	"fmt"
	"github.com/ihsw/go-download/log"
	"github.com/ihsw/go-download/util"
)

var status_url = "http://us.battle.net/api/wow/realm/status"

func main() {
	util.Write("Starting...")

	l := log.New("127.0.0.1:6379", "", 0, "jello")
	l.Write("Jello")

	util.Write(fmt.Sprintf("Downloading from %s...", status_url))
	contents, err := util.Download(status_url)
	if err != nil {
		util.Write(fmt.Sprintf("Failed! %s", err))
		return
	}

	util.Write("Dumping...")
	fmt.Println(contents["realms"])
	for k, _ := range contents {
		fmt.Println("sup", k)
	}

	util.Write("Success")
}
