package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/util"
)

func getItemFilepath(c Config, ID blizzard.ItemID) (string, error) {
	return filepath.Abs(
		fmt.Sprintf("%s/items/%d.json", c.CacheDir, ID),
	)
}

type getItemsJob struct {
	Err  error
	ID   blizzard.ItemID
	Item blizzard.Item
}

func GetItems(IDs []blizzard.ItemID, ibMap state.ItemBlacklistMap, res Resolver) chan getItemsJob {
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

	// queueing up the Realms
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

func getItem(ID blizzard.ItemID, res Resolver) (blizzard.Item, error) {
	if res.Config == nil {
		return blizzard.Item{}, errors.New("Config cannot be nil")
	}

	if res.Config.CacheDir == "" {
		return blizzard.Item{}, errors.New("Cache dir cannot be blank")
	}

	itemFilepath, err := getItemFilepath(*res.Config, ID)
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
			"Item":     ID,
			"filepath": itemFilepath,
		}).Debug("Loading Item from filepath")

		return blizzard.NewItemFromFilepath(itemFilepath)
	}

	// optionally checking gcloud Store
	if res.Config.UseGCloud {
		exists, err := res.Store.ItemExists(ID)
		if err != nil {
			return blizzard.Item{}, err
		}

		if exists {
			logging.WithField("Item", ID).Debug("Loading Item from gcloud storage")

			return blizzard.NewItemFromGcloudObject(res.Store.Context, res.Store.GetItemObject(ID))
		}
	}

	logging.WithField("Item", ID).Debug("Fetching Item")

	// checking blizzard api
	primaryRegion, err := res.Config.Regions.GetPrimaryRegion()
	if err != nil {
		return blizzard.Item{}, err
	}

	uri, err := res.AppendAccessToken(res.GetItemURL(primaryRegion.Hostname, ID))
	if err != nil {
		return blizzard.Item{}, err
	}

	item, resp, err := blizzard.NewItemFromHTTP(uri)
	if err != nil {
		return blizzard.Item{}, err
	}

	// optionally writing it back to gcloud Store or disk
	if res.Config.UseGCloud {
		encodedBody, err := util.GzipEncode(resp.Body)
		if err != nil {
			return blizzard.Item{}, err
		}

		if err := res.Store.WriteItem(ID, encodedBody); err != nil {
			return blizzard.Item{}, err
		}
	}

	if err := util.WriteFile(itemFilepath, resp.Body); err != nil {
		return blizzard.Item{}, err
	}

	return item, nil
}

func NewItemIdsMap(IDs []blizzard.ItemID) itemIdsMap {
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

func NewItemsMapFromGzipped(body []byte) (ItemsMap, error) {
	gzipDecodedData, err := util.GzipDecode(body)
	if err != nil {
		return ItemsMap{}, err
	}

	return newItemsMap(gzipDecodedData)
}

func newItemsMap(body []byte) (ItemsMap, error) {
	iMap := &ItemsMap{}
	if err := json.Unmarshal(body, iMap); err != nil {
		return nil, err
	}

	return *iMap, nil
}

type ItemsMap map[blizzard.ItemID]Item

func (iMap ItemsMap) getItemIds() []blizzard.ItemID {
	out := []blizzard.ItemID{}
	for ID := range iMap {
		out = append(out, ID)
	}

	return out
}

func (iMap ItemsMap) GetItemIconsMap(excludeWithURL bool) itemIconItemIdsMap {
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

func (iMap ItemsMap) EncodeForDatabase() ([]byte, error) {
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

type Item struct {
	blizzard.Item

	IconURL string `json:"icon_url"`
}

type itemIds []blizzard.ItemID

type itemIconItemIdsMap map[string]itemIds

func (iconsMap itemIconItemIdsMap) GetItemIcons() []string {
	iconNames := make([]string, len(iconsMap))
	i := 0
	for iconName := range iconsMap {
		iconNames[i] = iconName

		i++
	}

	return iconNames
}
