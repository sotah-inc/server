package main

import (
	"fmt"
	"github.com/ihsw/go-download/util"
	"reflect"
)

var status_url string = "http://us.battle.net/api/wow/realm/status"

func main() {
	util.Write(fmt.Sprintf("Reading from %s...", status_url))
	contents, err := util.Download(status_url)
	if err != nil {
		util.Write(fmt.Sprintf("Failed! %s", err))
		return
	}

	util.Write(reflect.TypeOf(contents))

	util.Write("Success!")
}
