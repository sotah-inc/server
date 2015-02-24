package main

import (
	"flag"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
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
		client  Cache.Client
		regions []Entity.Region
		err     error
	)
	if client, regions, err = Misc.Init(*configPath, *flushDb); err != nil {
		output.Write(fmt.Sprintf("Misc.Init() fail: %s", err.Error()))
		return
	}

	for _, region := range regions {
		var response Status.Response
		if response, err = Status.Get(region, client.ApiKey); err != nil {
			output.Write(fmt.Sprintf("Status.Get() fail: %s", err.Error()))
			return
		}

		realmManger := Entity.NewRealmManager(client)
		realms := make([]Entity.Realm, len(response.Realms))
		for i, responseRealm := range response.Realms {
			realms[i] = Entity.Realm{
				Name:        responseRealm.Name,
				Slug:        responseRealm.Slug,
				Battlegroup: responseRealm.Battlegroup,
				Type:        responseRealm.Type,
				Status:      responseRealm.Status,
				Population:  responseRealm.Population,
			}
		}
		if realms, err = realmManger.PersistAll(realms); err != nil {
			output.Write(fmt.Sprintf("RealmManager.PersistAll() fail: %s", err.Error()))
			return
		}
	}

	output.Conclude()
}
