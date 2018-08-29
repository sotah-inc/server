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

func getItemFilepath(c config, ID blizzard.ItemID) (string, error) {
	return filepath.Abs(
		fmt.Sprintf("%s/items/%d.json", c.CacheDir, ID),
	)
}

type loadItemsJob struct {
	err      error
	filepath string
	item     blizzard.Item
	iconURL  string
}

func loadItems(res resolver) (chan loadItemsJob, error) {
	if res.config.UseGCloudStorage {
		return res.store.loadItems(res)
	}

	return loadItemsFromFilecache(*res.config)
}

func loadItemsFromFilecache(c config) (chan loadItemsJob, error) {
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
	err     error
	ID      blizzard.ItemID
	item    blizzard.Item
	iconURL string
}

func getItems(IDs []blizzard.ItemID, res resolver) (chan getItemsJob, error) {
	if res.config.UseGCloudStorage {
		return res.store.getItems(IDs, res)
	}

	return getItemsFromFilecache(IDs, res), nil
}

func getItemsFromFilecache(IDs []blizzard.ItemID, res resolver) chan getItemsJob {
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

	if res.config.CacheDir == "" {
		return blizzard.Item{}, errors.New("Cache dir cannot be blank")
	}

	itemFilepath, err := getItemFilepath(*res.config, ID)
	if err != nil {
		return blizzard.Item{}, err
	}

	if _, err := os.Stat(itemFilepath); err != nil {
		if !os.IsNotExist(err) {
			return blizzard.Item{}, err
		}

		// optionally checking gcloud store
		if res.config.UseGCloudStorage {
			exists, err := res.store.itemExists(ID)
			if err != nil {
				return blizzard.Item{}, err
			}
			if exists {
				itemValue, _, err := res.store.loadItem(ID, res)
				if err != nil {
					return blizzard.Item{}, err
				}

				return itemValue, nil
			}
		}

		log.WithField("item", ID).Info("Fetching item")

		// checking blizzard api
		primaryRegion, err := res.config.Regions.getPrimaryRegion()
		if err != nil {
			return blizzard.Item{}, err
		}
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

		// writing it back to disk
		if err := util.WriteFile(itemFilepath, resp.Body); err != nil {
			return blizzard.Item{}, err
		}

		// optionally writing it back to gcloud store
		if res.config.UseGCloudStorage {
			encodedBody, err := util.GzipEncode(resp.Body)
			if err != nil {
				return blizzard.Item{}, err
			}

			if err := res.store.writeItem(ID, encodedBody); err != nil {
				return blizzard.Item{}, err
			}
		}

		return item, nil
	}

	return blizzard.NewItemFromFilepath(itemFilepath)
}

type itemsMap map[blizzard.ItemID]item

func (iMap itemsMap) getItemIconsMap(excludeWithURL bool) itemIconItemIdsMap {
	iconsMap := map[string]itemIds{}
	for itemID, iValue := range iMap {
		if excludeWithURL && iValue.IconURL != "" {
			continue
		}

		if iValue.Icon == "" {
			continue
		}

		if _, ok := iconsMap[iValue.Icon]; !ok {
			iconsMap[iValue.Icon] = itemIds{itemID}

			continue
		}

		iconsMap[iValue.Icon] = append(iconsMap[iValue.Icon], itemID)
	}

	return iconsMap
}

type item struct {
	blizzard.Item

	IconURL string `json:"icon_url"`
}

type itemIds []blizzard.ItemID
type itemIconItemIdsMap map[string]itemIds

func (iconsMap itemIconItemIdsMap) getItemIcons() []string {
	iconNames := make([]string, len(iconsMap))
	i := 0
	for iconName := range iconsMap {
		iconNames[i] = iconName

		i++
	}

	return iconNames
}
