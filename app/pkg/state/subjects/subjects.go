package subjects

// Subject - typehint for these enums
type Subject string

/*
Status - subject name for returning current status
*/
const (
	Status                   Subject = "status"
	Auctions                 Subject = "auctions"
	GenericTestErrors        Subject = "genericTestErrors"
	Owners                   Subject = "owners"
	OwnersQueryByItems       Subject = "ownersQueryByItems"
	OwnersQuery              Subject = "ownersQuery"
	ItemsQuery               Subject = "itemsQuery"
	PriceList                Subject = "priceList"
	PriceListHistory         Subject = "priceListHistory"
	Items                    Subject = "items"
	Boot                     Subject = "boot"
	SessionSecret            Subject = "sessionSecret"
	RuntimeInfo              Subject = "runtimeInfo"
	LiveAuctionsIntake       Subject = "liveAuctionsIntake"
	PricelistHistoriesIntake Subject = "pricelistHistoriesIntake"
	AppMetrics               Subject = "appMetrics"
	AuctionCount             Subject = "auctionCount"
	AuctionCountReceive      Subject = "auctionCountReceive"
)
