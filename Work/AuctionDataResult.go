package Work

import (
	"github.com/ihsw/go-download/Entity"
)

/*
	funcs
*/
func NewAuctionDataResult(realm Entity.Realm) AuctionDataResult {
	return AuctionDataResult{Result: NewResult(realm)}
}

/*
	AuctionDataResult
*/
type AuctionDataResult struct {
	Result
	ResponseFailed bool
	AlreadyChecked bool
}

func (self AuctionDataResult) CanContinue() bool {
	return !self.AlreadyChecked && !self.ResponseFailed
}
