package commands

type command string

/*
Commands - commands that run on main
*/
var (
	API                command = "api"
	LiveAuctions       command = "live-auctions"
	PricelistHistories command = "pricelist-histories"

	ProdApi                command = "prod-api"
	ProdMetrics            command = "prod-metrics"
	ProdLiveAuctions       command = "prod-live-auctions"
	ProdPricelistHistories command = "prod-pricelist-histories"
	ProdItems              command = "prod-items"
)
