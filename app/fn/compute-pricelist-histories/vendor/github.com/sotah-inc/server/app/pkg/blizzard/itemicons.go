package blizzard

import (
	"fmt"
)

const itemIconURLFormat = "https://render-us.worldofwarcraft.com/icons/56/%s.jpg"
const characterIconURLFormat = "https://render-us.worldofwarcraft.com/character/%s"
const characterAvatarURLFormat = "https://render-%s.worldofwarcraft.com/character/%s/%d/%d-avatar.jpg"
const characterMainURLFormat = "https://render-%s.worldofwarcraft.com/character/%s/%d/%d-main.jpg"
const characterInsetURLFormat = "https://render-%s.worldofwarcraft.com/character/%s/%d/%d-inset.jpg"

func DefaultGetItemIconURL(name string) string {
	return fmt.Sprintf(itemIconURLFormat, name)
}

type GetItemIconURLFunc func(string) string

func DefaultGetCharacterIconURLFunc(name string) string {
	return fmt.Sprintf(characterIconURLFormat, name)
}

type GetCharacterIconURLFunc func(string) string
