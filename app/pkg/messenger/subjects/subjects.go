package subjects

// Subject - typehint for these enums
type Subject string

/*
Status - subject name for returning current status
*/
const (
	Status             Subject = "status"
	Auctions           Subject = "auctions"
	AuctionsIntake     Subject = "auctionsIntake"
	PricelistsIntake   Subject = "pricelistsIntake"
	Regions            Subject = "regions"
	GenericTestErrors  Subject = "genericTestErrors"
	Owners             Subject = "owners"
	OwnersQueryByItems Subject = "ownersQueryByItems"
	OwnersQuery        Subject = "ownersQuery"
	ItemsQuery         Subject = "itemsQuery"
	ItemClasses        Subject = "itemClasses"
	PriceList          Subject = "priceList"
	PriceListHistory   Subject = "priceListHistory"
	Items              Subject = "items"
	Info               Subject = "info"
	Boot               Subject = "boot"
	SessionSecret      Subject = "sessionSecret"
	RuntimeInfo        Subject = "runtimeInfo"
	LiveAuctionsIntake Subject = "liveAuctionsIntake"
)
