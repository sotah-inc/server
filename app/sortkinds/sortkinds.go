package sortkinds

// SortKind - typehint for these enums
type SortKind int

/*
Sortkinds - types of auction sorting
*/
const (
	None SortKind = iota
	Item
	Quantity
	Bid
	Buyout
	BuyoutPer
	Auctions
	Owner
)
