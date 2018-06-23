package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/ihsw/sotah-server/app/itembinds"
	"github.com/ihsw/sotah-server/app/util"

	log "github.com/sirupsen/logrus"
)

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

type itemID int64
type inventoryType int

type itemSpellID int
type itemSpellSpell struct {
	ID          itemSpellID `json:"id"`
	Name        string      `json:"name"`
	Icon        string      `json:"icon"`
	Description string      `json:"description"`
	CastTime    string      `json:"castTime"`
}

type itemSpell struct {
	SpellID    itemSpellID    `json:"spellId"`
	NCharges   int            `json:"nCharges"`
	Consumable bool           `json:"consumable"`
	CategoryID int            `json:"categoryId"`
	Trigger    string         `json:"trigger"`
	Spell      itemSpellSpell `json:"spell"`
}

type itemWeaponDamage struct {
	Min      int     `json:"min"`
	Max      int     `json:"max"`
	ExactMin float32 `json:"exactMin"`
	ExactMax float32 `json:"exactMax"`
}

type itemWeaponInfo struct {
	Damage      itemWeaponDamage `json:"damage"`
	WeaponSpeed float32          `json:"weaponSpeed"`
	Dps         float32          `json:"dps"`
}

type itemBonusStat struct {
	Stat   int `json:"stat"`
	Amount int `json:"amount"`
}

type item struct {
	ID             itemID             `json:"id"`
	Name           string             `json:"name"`
	Quality        int                `json:"quality"`
	NormalizedName string             `json:"normalized_name"`
	Icon           string             `json:"icon"`
	ItemLevel      int                `json:"itemLevel"`
	ItemClass      itemClassClass     `json:"itemClass"`
	ItemSubClass   itemSubClassClass  `json:"itemSubClass"`
	InventoryType  inventoryType      `json:"inventoryType"`
	ItemBind       itembinds.ItemBind `json:"itemBind"`
	RequiredLevel  int                `json:"requiredLevel"`
	Armor          int                `json:"armor"`
	MaxDurability  int                `json:"maxDurability"`
	SellPrice      int                `json:"sellPrice"`
	ItemSpells     []itemSpell        `json:"itemSpells"`
	Equippable     bool               `json:"equippable"`
	Stackable      int                `json:"stackable"`
	WeaponInfo     itemWeaponInfo     `json:"weaponInfo"`
	BonusStats     []itemBonusStat    `json:"bonusStats"`
	Description    string             `json:"description"`
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
