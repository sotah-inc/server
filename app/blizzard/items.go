package blizzard

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/ihsw/sotah-server/app/blizzard/itembinds"
	"github.com/ihsw/sotah-server/app/util"
)

const itemURLFormat = "https://%s/wow/item/%d"

func defaultGetItemURL(regionHostname string, ID itemID) string {
	return fmt.Sprintf(itemURLFormat, regionHostname, ID)
}

type getItemURLFunc func(string, itemID) string

func newItemFromHTTP(uri string) (item, error) {
	body, err := util.Download(uri)
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
