package main

import (
	"fmt"
)

const itemIconURLFormat = "https://render-us.worldofwarcraft.com/icons/56/%s.jpg"

func defaultGetItemIconURL(name string) string {
	return fmt.Sprintf(itemIconURLFormat, name)
}

type getItemIconURLFunc func(string) string
