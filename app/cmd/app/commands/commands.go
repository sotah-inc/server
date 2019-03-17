package commands

type command string

/*
Commands - commands that run on main
*/
var (
	API                command = "api"
	ProdApi            command = "prod-api"
	ProdMetrics        command = "prod-metrics"
	ProdLiveAuctions   command = "prod-live-auctions"
	LiveAuctions       command = "live-auctions"
	PricelistHistories command = "pricelist-histories"
	Pub                command = "pub"
)
