package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/util"

	log "github.com/sirupsen/logrus"
)

type loadItemsJob struct {
	err      error
	filepath string
	item     blizzard.Item
}

func loadItems(c config) (chan loadItemsJob, error) {
	// listing out files in items dir
	itemsDirPath, err := filepath.Abs(fmt.Sprintf("%s/items", c.CacheDir))
	if err != nil {
		return nil, err
	}
	itemsFilepaths, err := ioutil.ReadDir(itemsDirPath)
	if err != nil {
		return nil, err
	}

	// establishing channels
	out := make(chan loadItemsJob)
	in := make(chan string)

	// spinning up the workers for fetching items
	worker := func() {
		for itemFilepath := range in {
			itemValue, err := blizzard.NewItemFromFilepath(itemFilepath)
			out <- loadItemsJob{err: err, item: itemValue, filepath: itemFilepath}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing up the realms
	go func() {
		itemsFilepathCount := len(itemsFilepaths)
		for i, itemFilepath := range itemsFilepaths {
			if i == 0 || i%5000 == 0 || i == itemsFilepathCount-1 {
				log.WithField("count", i).Debug("Loaded items")
			}

			filename := itemFilepath.Name()
			if filename == ".gitkeep" {
				continue
			}

			in <- fmt.Sprintf("%s/%s", itemsDirPath, filename)
		}

		close(in)
	}()

	return out, nil
}

type getItemsJob struct {
	err  error
	ID   blizzard.ItemID
	item blizzard.Item
}

func getItems(IDs []blizzard.ItemID, res resolver) chan getItemsJob {
	// establishing channels
	out := make(chan getItemsJob)
	in := make(chan blizzard.ItemID)

	// spinning up the workers for fetching items
	worker := func() {
		for ID := range in {
			itemValue, err := getItem(ID, res)
			out <- getItemsJob{err: err, item: itemValue, ID: ID}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing up the realms
	go func() {
		for _, ID := range IDs {
			in <- ID
		}

		close(in)
	}()

	return out
}

func getItem(ID blizzard.ItemID, res resolver) (blizzard.Item, error) {
	if res.config == nil {
		return blizzard.Item{}, errors.New("Config cannot be nil")
	}

	primaryRegion, err := res.config.Regions.getPrimaryRegion()
	if err != nil {
		return blizzard.Item{}, err
	}

	if res.config.UseCacheDir == false {
		uri, err := res.appendAPIKey(blizzard.DefaultGetItemURL(primaryRegion.Hostname, ID))
		if err != nil {
			return blizzard.Item{}, err
		}

		item, resp, err := blizzard.NewItemFromHTTP(uri)
		if err != nil {
			return blizzard.Item{}, err
		}
		if err := res.messenger.publishPlanMetaMetric(resp); err != nil {
			return blizzard.Item{}, err
		}

		return item, nil
	}

	if res.config.CacheDir == "" {
		return blizzard.Item{}, errors.New("Cache dir cannot be blank")
	}

	itemFilepath, err := filepath.Abs(
		fmt.Sprintf("%s/items/%d.json", res.config.CacheDir, ID),
	)
	if err != nil {
		return blizzard.Item{}, err
	}

	if _, err := os.Stat(itemFilepath); err != nil {
		if !os.IsNotExist(err) {
			return blizzard.Item{}, err
		}

		log.WithField("item", ID).Info("Fetching item")

		uri, err := res.appendAPIKey(res.getItemURL(primaryRegion.Hostname, ID))
		if err != nil {
			return blizzard.Item{}, err
		}

		item, resp, err := blizzard.NewItemFromHTTP(uri)
		if err != nil {
			return blizzard.Item{}, err
		}
		if err := res.messenger.publishPlanMetaMetric(resp); err != nil {
			return blizzard.Item{}, err
		}

		if err := util.WriteFile(itemFilepath, resp.Body); err != nil {
			return blizzard.Item{}, err
		}

		return item, nil
	}

	return blizzard.NewItemFromFilepath(itemFilepath)
}

type itemsMap map[blizzard.ItemID]blizzard.Item

func (iMap itemsMap) getItemIcons() []string {
	iconsMap := map[string]struct{}{}
	for _, iValue := range iMap {
		if iValue.Icon == "" {
			continue
		}

		iconsMap[iValue.Icon] = struct{}{}
	}

	i := 0
	out := make([]string, len(iconsMap))
	for iconName := range iconsMap {
		out[i] = iconName
		i++
	}

	return out
}
