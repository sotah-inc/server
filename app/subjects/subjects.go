package subjects

// Subject - typehint for these enums
type Subject string

/*
Status - subject name for returning current status
*/
const (
	Status            Subject = "status"
	Auctions          Subject = "auctions"
	AuctionsIntake    Subject = "auctionsIntake"
	PricelistsIntake  Subject = "pricelistsIntake"
	Regions           Subject = "regions"
	GenericTestErrors Subject = "genericTestErrors"
	Owners            Subject = "owners"
	ItemsQuery        Subject = "itemsQuery"
	AuctionsQuery     Subject = "auctionsQuery"
	ItemClasses       Subject = "itemClasses"
	PriceList         Subject = "priceList"
	PriceListHistory  Subject = "priceListHistory"
	Items             Subject = "items"
	Info              Subject = "info"
	AppMetrics        Subject = "appMetrics"
	Boot              Subject = "boot"
)
