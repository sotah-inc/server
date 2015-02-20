package Work

/*
	funcs
*/
func NewItemizeResult(auctionDataResult AuctionDataResult) ItemizeResult {
	return ItemizeResult{AuctionDataResult: auctionDataResult}
}

/*
	ItemizeResult
*/
type ItemizeResult struct {
	AuctionDataResult
	blizzItemIds []int64
}
