package main

import (
	"fmt"
)

const itemIconURLFormat = "https://render-us.worldofwarcraft.com/icons/56/%s.jpg"
const characterIconURLFormat = "https://render-us.worldofwarcraft.com/character/%s"

func defaultGetItemIconURL(name string) string {
	return fmt.Sprintf(itemIconURLFormat, name)
}

type getItemIconURLFunc func(string) string
