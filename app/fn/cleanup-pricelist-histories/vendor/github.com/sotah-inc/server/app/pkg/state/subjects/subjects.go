package subjects

// Subject - typehint for these enums
type Subject string

/*
Status - subject name for returning current status
*/
const (
	Status                          Subject = "status"
	Auctions                        Subject = "auctions"
	GenericTestErrors               Subject = "genericTestErrors"
	Owners                          Subject = "owners"
	OwnersQueryByItems              Subject = "ownersQueryByItems"
	OwnersQuery                     Subject = "ownersQuery"
	ItemsQuery                      Subject = "itemsQuery"
	PriceList                       Subject = "priceList"
	PriceListHistory                Subject = "priceListHistory"
	PriceListHistoryV2              Subject = "priceListHistoryV2"
	Items                           Subject = "items"
	Boot                            Subject = "boot"
	SessionSecret                   Subject = "sessionSecret"
	RuntimeInfo                     Subject = "runtimeInfo"
	LiveAuctionsIntake              Subject = "liveAuctionsIntake"
	LiveAuctionsCompute             Subject = "liveAuctionsCompute"
	PricelistHistoriesIntake        Subject = "pricelistHistoriesIntake"
	AppMetrics                      Subject = "appMetrics"
	PricelistHistoriesIntakeV2      Subject = "pricelistHistoriesIntakeV2"
	PricelistHistoriesCompute       Subject = "pricelistHistoriesCompute"
	PricelistHistoriesComputeIntake Subject = "pricelistHistoriesComputeIntake"
)

// gcloud fn-related
const (
	DownloadAllAuctions Subject = "downloadAllAuctions"
	DownloadAuctions    Subject = "downloadAuctions"

	ReceiveComputedLiveAuctions       Subject = "receiveComputedLiveAuctions"
	ReceiveComputedPricelistHistories Subject = "receiveComputedPricelistHistories"

	FilterInItemsToSync Subject = "filterInItemsToSync"
	SyncAllItems        Subject = "syncAllItems"
	SyncItems           Subject = "syncItems"
	SyncItemIcons       Subject = "syncItemIcons"
	ReceiveSyncedItems  Subject = "receiveSyncedItems"

	CleanupAllExpiredManifests Subject = "cleanupAllExpiredManifests"
	CleanupExpiredManifest     Subject = "cleanupExpiredManifest"

	Bullshit       Subject = "bullshit"
	BullshitIntake Subject = "bullshitIntake"
)
