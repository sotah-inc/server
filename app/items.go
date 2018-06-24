package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/util"

	log "github.com/sirupsen/logrus"
)

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
		return blizzard.NewItemFromHTTP(blizzard.DefaultGetItemURL(primaryRegion.Hostname, ID))
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

		body, err := res.get(res.getItemURL(primaryRegion.Hostname, ID))
		if err != nil {
			return blizzard.Item{}, err
		}

		if err := util.WriteFile(itemFilepath, body); err != nil {
			return blizzard.Item{}, err
		}

		return blizzard.NewItem(body)
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
