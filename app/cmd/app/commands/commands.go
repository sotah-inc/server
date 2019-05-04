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

	FnDownloadAllAuctions          command = "fn-download-all-auctions"
	FnComputeAllLiveAuctions       command = "fn-compute-all-live-auctions"
	FnComputeAllPricelistHistories command = "fn-compute-all-pricelist-histories"
	FnSyncAllItems                 command = "fn-sync-all-items"
	FnCleanupAllExpiredManifests   command = "fn-cleanup-all-expired-manifests"
	FnCleanupPricelistHistories    command = "fn-cleanup-pricelist-histories"
)
