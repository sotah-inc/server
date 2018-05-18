package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"

	log "github.com/sirupsen/logrus"
)

type itemID int64

const itemURLFormat = "https://%s/wow/item/%d"

func defaultGetItemURL(regionHostname string, ID itemID) string {
	return fmt.Sprintf(itemURLFormat, regionHostname, ID)
}

type getItemURLFunc func(string, itemID) string

func newItemFromHTTP(ID itemID, r resolver) (item, error) {
	if r.config == nil {
		return item{}, errors.New("Config cannot be nil")
	}

	primaryRegion, err := r.config.Regions.getPrimaryRegion()
	if err != nil {
		return item{}, err
	}

	body, err := r.get(r.getItemURL(primaryRegion.Hostname, ID))
	if err != nil {
		return item{}, err
	}

	return newItem(body)
}

func newItemFromFilepath(relativeFilepath string) (item, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return item{}, err
	}

	return newItem(body)
}

func newItem(body []byte) (item, error) {
	i := &item{}
	if err := json.Unmarshal(body, i); err != nil {
		return item{}, err
	}

	reg, err := regexp.Compile("[^a-z0-9 ]+")
	if err != nil {
		return item{}, err
	}

	if i.NormalizedName == "" {
		i.NormalizedName = reg.ReplaceAllString(strings.ToLower(i.Name), "")
	}

	return *i, nil
}

type item struct {
	ID             itemID `json:"id"`
	Name           string `json:"name"`
	NormalizedName string `json:"normalized_name"`
}

type getItemsJob struct {
	err  error
	ID   itemID
	item item
}

func getItems(IDs []itemID, res resolver) chan getItemsJob {
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

func getItem(ID itemID, res resolver) (item, error) {
	if res.config == nil {
		return item{}, errors.New("Config cannot be nil")
	}

	if res.config.UseCacheDir == false {
		return newItemFromHTTP(ID, res)
	}

	if res.config.CacheDir == "" {
		return item{}, errors.New("Cache dir cannot be blank")
	}

	itemFilepath, err := filepath.Abs(
		fmt.Sprintf("%s/items/%d.json", res.config.CacheDir, ID),
	)
	if err != nil {
		return item{}, err
	}

	if _, err := os.Stat(itemFilepath); err != nil {
		if !os.IsNotExist(err) {
			return item{}, err
		}

		primaryRegion, err := res.config.Regions.getPrimaryRegion()
		if err != nil {
			return item{}, err
		}

		log.WithField("item", ID).Info("Fetching item")

		body, err := res.get(res.getItemURL(primaryRegion.Hostname, ID))
		if err != nil {
			return item{}, err
		}

		if err := util.WriteFile(itemFilepath, body); err != nil {
			return item{}, err
		}

		return newItem(body)
	}

	return newItemFromFilepath(itemFilepath)
}

type itemsMap map[itemID]item

func newItemListResultFromMessenger(mess messenger, request itemsRequest) (itemListResult, error) {
	encodedMessage, err := json.Marshal(request)
	if err != nil {
		return itemListResult{}, err
	}

	msg, err := mess.request(subjects.Items, encodedMessage)
	if err != nil {
		return itemListResult{}, err
	}

	if msg.Code != codes.Ok {
		return itemListResult{}, errors.New(msg.Err)
	}

	return newItemListResult([]byte(msg.Data))
}

func newItemListResultFromFilepath(relativeFilepath string) (itemListResult, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return itemListResult{}, err
	}

	return newItemListResult(body)
}

func newItemListResult(body []byte) (itemListResult, error) {
	i := &itemListResult{}
	if err := json.Unmarshal(body, i); err != nil {
		return itemListResult{}, err
	}

	return *i, nil
}

type itemListResult struct {
	Items itemList `json:"items"`
}

type itemList []item

func (il itemList) limit() itemList {
	listLength := len(il)
	if listLength > 10 {
		listLength = 10
	}

	out := make(itemList, listLength)
	for i := 0; i < listLength; i++ {
		out[i] = il[i]
	}

	return out
}

func (il itemList) filter(query string) itemList {
	lowerQuery := strings.ToLower(query)
	matches := itemList{}
	for _, itemValue := range il {
		if !strings.Contains(itemValue.NormalizedName, lowerQuery) {
			continue
		}

		matches = append(matches, itemValue)
	}

	return matches
}

type itemsByName itemList

func (by itemsByName) Len() int           { return len(by) }
func (by itemsByName) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by itemsByName) Less(i, j int) bool { return by[i].Name < by[j].Name }

type itemsByNormalizedName itemList

func (by itemsByNormalizedName) Len() int      { return len(by) }
func (by itemsByNormalizedName) Swap(i, j int) { by[i], by[j] = by[j], by[i] }
func (by itemsByNormalizedName) Less(i, j int) bool {
	return by[i].NormalizedName < by[j].NormalizedName
}
