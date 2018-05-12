package main

type itemID int64

type item struct {
	ItemID itemID `json:"item_id"`
	Name   string `json:"name"`
}

type items struct {
	Items itemsList `json:"items"`
}

type itemsList []item
