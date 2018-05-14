package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ihsw/sotah-server/app/util"

	log "github.com/sirupsen/logrus"
)

type itemID int64

const itemURLFormat = "https://%s/wow/item/%d"

func defaultGetItemURL(regionHostname string, ID itemID) string {
	return fmt.Sprintf(itemURLFormat, regionHostname, ID)
}

type getItemURLFunc func(string, itemID) string

func newItemFromHTTP(ID itemID, r *resolver) (*item, error) {
	if r.config == nil {
		return nil, errors.New("Config cannot be nil")
	}

	primaryRegion, err := r.config.Regions.getPrimaryRegion()
	if err != nil {
		return nil, err
	}

	body, err := r.get(r.getItemURL(primaryRegion.Hostname, ID))
	if err != nil {
		return nil, err
	}

	return newItem(body)
}

func newItemFromFilepath(relativeFilepath string) (*item, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return nil, err
	}

	return newItem(body)
}

func newItem(body []byte) (*item, error) {
	i := &item{}
	if err := json.Unmarshal(body, i); err != nil {
		return nil, err
	}

	return i, nil
}

type item struct {
	ID   itemID `json:"id"`
	Name string `json:"name"`
}

type getItemsJob struct {
	err  error
	ID   itemID
	item *item
}

func getItems(IDs []itemID, res *resolver) chan getItemsJob {
	// establishing channels
	out := make(chan getItemsJob)
	in := make(chan itemID)

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

func getItem(ID itemID, res *resolver) (*item, error) {
	if res.config == nil {
		return nil, errors.New("Config cannot be nil")
	}

	if res.config.UseCacheDir == false {
		return newItemFromHTTP(ID, res)
	}

	if res.config.CacheDir == "" {
		return nil, errors.New("Cache dir cannot be blank")
	}

	itemFilepath, err := filepath.Abs(
		fmt.Sprintf("%s/items/%d.json", res.config.CacheDir, ID),
	)
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(itemFilepath); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		primaryRegion, err := res.config.Regions.getPrimaryRegion()
		if err != nil {
			return nil, err
		}

		log.WithField("item", ID).Info("Fetching item")

		body, err := res.get(res.getItemURL(primaryRegion.Hostname, ID))
		if err != nil {
			return nil, err
		}

		if err := util.WriteFile(itemFilepath, body); err != nil {
			return nil, err
		}

		return newItem(body)
	}

	return newItemFromFilepath(itemFilepath)
}

type itemsMap map[itemID]*item
