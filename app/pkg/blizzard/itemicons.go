package blizzard

import (
	"fmt"
)

const itemIconURLFormat = "https://render-us.worldofwarcraft.com/icons/56/%s.jpg"
const characterIconURLFormat = "https://render-us.worldofwarcraft.com/character/%s"

func DefaultGetItemIconURL(name string) string {
	return fmt.Sprintf(itemIconURLFormat, name)
}

type GetItemIconURLFunc func(string) string

func DefaultGetCharacterIconURLFunc(name string) string {
	return fmt.Sprintf(characterIconURLFormat, name)
}

type GetCharacterIconURLFunc func(string) string
