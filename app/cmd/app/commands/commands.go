package commands

type command string

/*
Commands - commands that run on main
*/
var (
	APITest            command = "api-test"
	API                command = "api"
	SyncItems          command = "sync-items"
	LiveAuctions       command = "live-auctions"
	PricelistHistories command = "pricelist-histories"
	PruneStore         command = "prune-store"
)
