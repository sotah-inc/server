package subjects

// Subject - typehint for these enums
type Subject string

/*
Status - subject name for returning current status
*/
const (
	Status            Subject = "status"
	Auctions          Subject = "auctions"
	Regions           Subject = "regions"
	GenericTestErrors Subject = "genericTestErrors"
	Owners            Subject = "owners"
	ItemsQuery        Subject = "itemsQuery"
	AuctionsQuery     Subject = "auctionsQuery"
	ItemClasses       Subject = "itemClasses"
)
