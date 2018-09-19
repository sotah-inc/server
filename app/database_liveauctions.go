package main

import (
	"github.com/boltdb/bolt"
)

type liveAuctionsDatabases map[regionName]*bolt.DB
