package main

import (
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Blizzard"
	"github.com/ihsw/go-download/Blizzard/Auction"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
	"io/ioutil"
	"os"
	"time"
)

func Initialize(args []string) (Config.ConfigFile, Cache.Client, error) {
	var (
		configFile Config.ConfigFile
		client     Cache.Client
		err        error
	)

	if len(args) == 1 {
		err = errors.New("Expected path to config file, got nothing")
		return configFile, client, err
	}

	// loading the config-file
	configFile, err = Config.New(args[1])
	if err != nil {
		return configFile, client, err
	}

	// connecting the redis clients
	client, err = Config.NewCacheClient(configFile.ConnectionList)
	if err != nil {
		return configFile, client, err
	}

	// flushing all of the databases
	err = client.FlushDb()
	if err != nil {
		return configFile, client, err
	}

	return configFile, client, nil
}

func Load(client Cache.Client, configRegions []Config.Region) ([]Entity.Region, error) {
	var (
		regions []Entity.Region
		err     error
	)
	regionManager := Entity.RegionManager{Client: client}
	localeManager := Entity.LocaleManager{Client: client}

	for _, configRegion := range configRegions {
		region := configRegion.ToEntity()
		region, err = regionManager.Persist(region)
		if err != nil {
			return regions, err
		}

		for _, configLocale := range configRegion.Locales {
			locale := configLocale.ToEntity()
			locale.Region = region
			locale, err = localeManager.Persist(locale)
			if err != nil {
				return regions, err
			}
		}
		regions = append(regions, region)
	}
	return regions, nil
}

func getRealms(client Cache.Client, regions []Entity.Region, statusDir string) (map[int64][]Entity.Realm, error) {
	var (
		regionRealms map[int64][]Entity.Realm
		err          error
	)
	regionRealms = map[int64][]Entity.Realm{}
	realmManager := Entity.RealmManager{Client: client}

	// going over the regions to download the statuses
	c := make(chan Status.Result, len(regions))
	for _, region := range regions {
		go Status.Get(region, statusDir, c)
	}

	// gathering the results
	results := make([]Status.Result, len(regions))
	for i := 0; i < len(results); i++ {
		results[i] = <-c
	}

	// going over the results
	for _, result := range results {
		if err = result.Error; err != nil {
			return regionRealms, err
		}

		region := result.Region
		responseRealms := result.Response.Realms
		for _, responseRealm := range responseRealms {
			realm := responseRealm.ToEntity()
			realm.Region = region
			realm, err = realmManager.Persist(realm)
			if err != nil {
				return regionRealms, err
			}

			regionRealms[region.Id] = append(regionRealms[region.Id], realm)
		}
	}

	return regionRealms, nil
}

func main() {
	output := Util.Output{
		StartTime: time.Now(),
	}
	output.Write("Starting...")

	var (
		err          error
		client       Cache.Client
		configFile   Config.ConfigFile
		regions      []Entity.Region
		regionRealms map[int64][]Entity.Realm
	)

	/*
		json dir handling
	*/
	// misc
	var fileinfo os.FileInfo
	directories := map[string]string{
		"json":            "json",
		"region-statuses": "json/region-statuses",
	}
	for _, directory := range directories {
		// checking whether it exists and creating where necessary
		fileinfo, err = os.Stat(directory)
		if err != nil {
			if !os.IsNotExist(err) {
				output.Write(fmt.Sprintf("os.Stat() fail: %s", err.Error()))
				return
			}

			err = os.MkdirAll(directory, 0755)
			if err != nil {
				output.Write(fmt.Sprintf("os.Mkdir() fail: %s", err.Error()))
				return
			}

			fileinfo, err = os.Stat(directory)
			if err != nil && !os.IsNotExist(err) {
				output.Write(fmt.Sprintf("os.Stat() fail: %s", err.Error()))
				return
			}
		}

		// checking whether it's a directory
		if !fileinfo.IsDir() {
			output.Write("json dir is not a directory!")
			return
		}

		// checking whether it's writeable with a test file
		testFilepath := fmt.Sprintf("%s/test", directory)
		err = ioutil.WriteFile(testFilepath, []byte("test"), 0755)
		if err != nil {
			output.Write(fmt.Sprintf("ioutil.WriteFile() fail: %s", err.Error()))
			return
		}

		// deleting the test file
		err = os.Remove(testFilepath)
		if err != nil {
			output.Write(fmt.Sprintf("os.Remove() fail: %s", err.Error()))
			return
		}
	}

	/*
		reading the config
	*/
	// getting a client
	output.Write("Initializing the config...")
	configFile, client, err = Initialize(os.Args)
	if err != nil {
		output.Write(fmt.Sprintf("initialize() fail: %s", err.Error()))
		return
	}

	// loading the regions and locales
	output.Write("Loading the regions and locales...")
	regions, err = Load(client, configFile.Regions)
	if err != nil {
		output.Write(fmt.Sprintf("load() fail: %s", err.Error()))
		return
	}

	/*
		gathering and persisting realms for each region
	*/
	output.Write("Fetching realms for each region...")
	regionRealms, err = getRealms(client, regions, directories["region-statuses"])
	if err != nil {
		output.Write(fmt.Sprintf("getRealms() fail: %s", err.Error()))
		return
	}

	/*
		removing realms that aren't queryable
	*/
	totalRealms := 0
	for _, region := range regions {
		if !region.Queryable {
			delete(regionRealms, region.Id)
			continue
		}
		totalRealms += len(regionRealms[region.Id])
	}
	totalRealms = 1

	regionMap := map[int64]int64{}
	for i, region := range regions {
		regionMap[region.Id] = int64(i)
	}

	/*
		checking the status of each realm
	*/
	// misc
	in := make(chan Entity.Realm, totalRealms)
	out := make(chan Auction.Result, totalRealms)
	workerCount := 8

	// spawning some workers
	output.Write("Spawning some workers...")
	for j := 0; j < workerCount; j++ {
		go func(in chan Entity.Realm, out chan Auction.Result) {
			for {
				Blizzard.DownloadRealm(<-in, out)
			}
		}(in, out)
	}

	// formatting the realms to be evenly distributed
	largestRegion := 0
	for _, realms := range regionRealms {
		if len(realms) > largestRegion {
			largestRegion = len(realms)
		}
	}
	formattedRealms := make([]map[int64]Entity.Realm, largestRegion)
	for regionId, realms := range regionRealms {
		for i, realm := range realms {
			if formattedRealms[int64(i)] == nil {
				formattedRealms[int64(i)] = map[int64]Entity.Realm{}
			}
			formattedRealms[int64(i)][regionId] = realm
		}
	}

	// queueing the realms up
	output.Write("Queueing up the realms for checking...")
	for _, realms := range formattedRealms {
		for _, realm := range realms {
			in <- realm
			break
		}
		break
	}

	// gathering the results
	output.Write("Gathering the results...")
	results := make([]Auction.Result, totalRealms)
	for i := 0; i < totalRealms; i++ {
		results[i] = <-out
	}

	// going over the results
	output.Write("Going over the results...")
	count := 0
	for _, result := range results {
		if err = result.Error; err != nil {
			output.Write(fmt.Sprintf("Auction.Get() fail: %s", err.Error()))
			return
		}

		fmt.Println(fmt.Sprintf("%#v", result.Response.Files))

		count++
	}
	output.Write(fmt.Sprintf("Count: %d", count))

	output.Conclude()
}
