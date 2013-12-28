package main

import (
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
	"github.com/ihsw/go-download/Work"
	"io/ioutil"
	"os"
	"time"
)

func initialize(args []string) (Config.ConfigFile, Cache.Client, error) {
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

func load(client Cache.Client, configRegions []Config.Region) ([]Entity.Region, error) {
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

func validateDirectories(directories map[string]string) error {
	var (
		err      error
		fileinfo os.FileInfo
	)
	for _, directory := range directories {
		// checking whether it exists and creating where necessary
		fileinfo, err = os.Stat(directory)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}

			err = os.MkdirAll(directory, 0755)
			if err != nil {
				return err
			}

			fileinfo, err = os.Stat(directory)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
		}

		// checking whether it's a directory
		if !fileinfo.IsDir() {
			return err
		}

		// checking whether it's writeable with a test file
		testFilepath := fmt.Sprintf("%s/test", directory)
		err = ioutil.WriteFile(testFilepath, []byte("test"), 0755)
		if err != nil {
			return err
		}

		// deleting the test file
		err = os.Remove(testFilepath)
		if err != nil {
			return err
		}
	}

	return nil
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
	directories := map[string]string{
		"json":            "json",
		"region-statuses": "json/region-statuses",
	}
	err = validateDirectories(directories)
	if err != nil {
		output.Write(fmt.Sprintf("validateDirectories() fail: %s", err.Error()))
		return
	}

	/*
		reading the config
	*/
	// getting a client
	output.Write("Initializing the config...")
	configFile, client, err = initialize(os.Args)
	if err != nil {
		output.Write(fmt.Sprintf("initialize() fail: %s", err.Error()))
		return
	}

	// loading the regions and locales
	output.Write("Loading the regions and locales...")
	regions, err = load(client, configFile.Regions)
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
		// totalRealms += len(regionRealms[region.Id])
		totalRealms += 1
	}

	regionMap := map[int64]int64{}
	for i, region := range regions {
		regionMap[region.Id] = int64(i)
	}

	/*
		making channels and spawning workers
	*/
	// misc
	downloadIn := make(chan Entity.Realm, totalRealms)
	itemizeIn := make(chan Work.DownloadResult, totalRealms)
	itemizeOut := make(chan Work.ItemizeResult, totalRealms)

	// spawning some download workers
	output.Write("Spawning some download workers...")
	downloadWorkerCount := 8
	for j := 0; j < downloadWorkerCount; j++ {
		go func(in chan Entity.Realm, out chan Work.DownloadResult, output Util.Output) {
			for {
				Work.DownloadRealm(<-in, out, output)
			}
		}(downloadIn, itemizeIn, output)
	}

	// spawning an itemize worker
	go func(in chan Work.DownloadResult, out chan Work.ItemizeResult) {
		for {
			Work.ItemizeRealm(<-in, out)
		}
	}(itemizeIn, itemizeOut)

	/*
		queueing up the realms
	*/
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

	// pushing the realms into the start of the queue
	output.Write("Queueing up the realms for checking...")
	for _, realms := range formattedRealms {
		for _, realm := range realms {
			downloadIn <- realm
		}
		break
	}

	/*
		debugging
	*/
	output.Write(fmt.Sprintf("Gathering %d results for debugging...", totalRealms))
	results := make([]Work.ItemizeResult, totalRealms)
	for i := 0; i < totalRealms; i++ {
		results[i] = <-itemizeOut
	}

	output.Write(fmt.Sprintf("Going over %d results for debugging...", len(results)))
	totalAuctionCount := 0
	for _, result := range results {
		realm := result.Realm
		if result.Error != nil {
			output.Write(fmt.Sprintf("Itemize %s fail: %s", realm.Dump(), result.Error.Error()))
			continue
		}

		totalAuctionCount += result.AuctionCount
		output.Write(fmt.Sprintf("%s has %d auctions...", realm.Dump(), result.AuctionCount))
	}
	output.Write(fmt.Sprintf("%d auctions in the world...", totalAuctionCount))

	output.Conclude()
}
