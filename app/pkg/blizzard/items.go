package blizzard

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/sotah-inc/server/app/pkg/blizzard/itembinds"
	"github.com/sotah-inc/server/app/pkg/util"
)

const itemURLFormat = "https://%s/wow/item/%d"

// DefaultGetItemURL generates a url according to the api format
func DefaultGetItemURL(regionHostname string, ID ItemID) string {
	return fmt.Sprintf(itemURLFormat, regionHostname, ID)
}

// GetItemURLFunc defines the expected func signature for generating an item uri
type GetItemURLFunc func(string, ItemID) string

// NewItemFromHTTP loads an item from the http api
func NewItemFromHTTP(uri string) (Item, ResponseMeta, error) {
	resp, err := Download(uri)
	if err != nil {
		return Item{}, ResponseMeta{}, err
	}

	if resp.Status != 200 {
		return Item{}, ResponseMeta{}, errors.New("status was not 200")
	}

	item, err := NewItem(resp.Body)
	if err != nil {
		return Item{}, ResponseMeta{}, err
	}

	return item, resp, nil
}

// NewItemFromFilepath loads an item from a json file
func NewItemFromFilepath(relativeFilepath string) (Item, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return Item{}, err
	}

	return NewItem(body)
}

// NewItem loads an item from a byte array of json
func NewItem(body []byte) (Item, error) {
	i := &Item{}
	if err := json.Unmarshal(body, i); err != nil {
		return Item{}, err
	}

	reg, err := regexp.Compile("[^a-z0-9 ]+")
	if err != nil {
		return Item{}, err
	}

	if i.NormalizedName == "" {
		i.NormalizedName = reg.ReplaceAllString(strings.ToLower(i.Name), "")
	}

	return *i, nil
}

// ItemID the api-specific identifier
type ItemID int64
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

// Item describes the item returned from the api
type Item struct {
	ID             ItemID             `json:"id"`
	Name           string             `json:"name"`
	Quality        int                `json:"quality"`
	NormalizedName string             `json:"normalized_name"`
	Icon           string             `json:"icon"`
	ItemLevel      int                `json:"itemLevel"`
	ItemClass      ItemClassClass     `json:"itemClass"`
	ItemSubClass   ItemSubClassClass  `json:"itemSubClass"`
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
