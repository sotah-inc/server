package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/blizzard"
	"github.com/sotah-inc/server/app/logging"
	"github.com/sotah-inc/server/app/util"
)

func getItemFilepath(c config, ID blizzard.ItemID) (string, error) {
	return filepath.Abs(
		fmt.Sprintf("%s/items/%d.json", c.CacheDir, ID),
	)
}

type getItemsJob struct {
	err  error
	ID   blizzard.ItemID
	item blizzard.Item
}

func getItems(IDs []blizzard.ItemID, ibMap itemBlacklistMap, res resolver) chan getItemsJob {
	// establishing channels
	out := make(chan getItemsJob)
	in := make(chan blizzard.ItemID)

	// spinning up the workers for fetching items
	worker := func() {
		for ID := range in {
			itemValue, err := getItem(ID, res)
			out <- getItemsJob{err, ID, itemValue}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing up the realms
	go func() {
		for _, ID := range IDs {
			if _, ok := ibMap[ID]; ok {
				continue
			}

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

	exists, err := func() (bool, error) {
		if _, err := os.Stat(itemFilepath); err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}

			return false, err
		}

		return true, nil
	}()
	if err != nil {
		return blizzard.Item{}, err
	}

	if exists {
		logging.WithFields(logrus.Fields{
			"item":     ID,
			"filepath": itemFilepath,
		}).Debug("Loading item from filepath")

		return blizzard.NewItemFromFilepath(itemFilepath)
	}

	// optionally checking gcloud store
	if res.config.UseGCloud {
		exists, err := res.store.itemExists(ID)
		if err != nil {
			return blizzard.Item{}, err
		}

		if exists {
			logging.WithField("item", ID).Debug("Loading item from gcloud storage")

			return blizzard.NewItemFromGcloudObject(res.store.context, res.store.getItemObject(ID))
		}
	}

	logging.WithField("item", ID).Debug("Fetching item")

	// checking blizzard api
	primaryRegion, err := res.config.Regions.getPrimaryRegion()
	if err != nil {
		return blizzard.Item{}, err
	}

	uri, err := res.appendAccessToken(res.getItemURL(primaryRegion.Hostname, ID))
	if err != nil {
		return blizzard.Item{}, err
	}

	item, resp, err := blizzard.NewItemFromHTTP(uri)
	if err != nil {
		return blizzard.Item{}, err
	}

	// optionally writing it back to gcloud store or disk
	if res.config.UseGCloud {
		encodedBody, err := util.GzipEncode(resp.Body)
		if err != nil {
			return blizzard.Item{}, err
		}

		if err := res.store.writeItem(ID, encodedBody); err != nil {
			return blizzard.Item{}, err
		}
	}

	if err := util.WriteFile(itemFilepath, resp.Body); err != nil {
		return blizzard.Item{}, err
	}

	return item, nil
}

func newItemIdsMap(IDs []blizzard.ItemID) itemIdsMap {
	out := itemIdsMap{}

	for _, ID := range IDs {
		out[ID] = struct{}{}
	}

	return out
}

type itemIdsMap map[blizzard.ItemID]struct{}

func (idsMap itemIdsMap) itemIds() []blizzard.ItemID {
	out := []blizzard.ItemID{}
	for ID := range idsMap {
		out = append(out, ID)
	}

	return out
}

func newItemsMapFromGzipped(body []byte) (itemsMap, error) {
	gzipDecodedData, err := util.GzipDecode(body)
	if err != nil {
		return itemsMap{}, err
	}

	return newItemsMap(gzipDecodedData)
}

func newItemsMap(body []byte) (itemsMap, error) {
	iMap := &itemsMap{}
	if err := json.Unmarshal(body, iMap); err != nil {
		return nil, err
	}

	return *iMap, nil
}

type itemsMap map[blizzard.ItemID]item

func (iMap itemsMap) getItemIds() []blizzard.ItemID {
	out := []blizzard.ItemID{}
	for ID := range iMap {
		out = append(out, ID)
	}

	return out
}

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

func (iMap itemsMap) encodeForDatabase() ([]byte, error) {
	jsonEncodedData, err := json.Marshal(iMap)
	if err != nil {
		return []byte{}, err
	}

	gzipEncodedData, err := util.GzipEncode(jsonEncodedData)
	if err != nil {
		return []byte{}, err
	}

	return gzipEncodedData, nil
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
