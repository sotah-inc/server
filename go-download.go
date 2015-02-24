package main

import (
	"flag"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Misc"
	"github.com/ihsw/go-download/Util"
	"runtime"
	"time"
)

type StatusGetResult struct {
	region   Entity.Region
	response Status.Response
	err      error
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flushDb := flag.Bool("flush", false, "Clears all redis dbs")
	configPath := flag.String("config", "", "Config path")
	// isProd := flag.Bool("prod", false, "Prod mode")
	flag.Parse()

	output := Util.Output{StartTime: time.Now()}
	output.Write("Starting...")

	// init
	var (
		regions      []Entity.Region
		regionRealms map[int64][]Entity.Realm
		err          error
	)
	if _, regions, regionRealms, err = Misc.Init(*configPath, *flushDb); err != nil {
		output.Write(fmt.Sprintf("Misc.Init() fail: %s", err.Error()))
		return
	}

	output.Write(fmt.Sprintf("Regions: %d", len(regions)))
	for _, region := range regions {
		output.Write(fmt.Sprintf("Realms in %s: %d", region.Name, len(regionRealms[region.Id])))
	}

	output.Conclude()
}
