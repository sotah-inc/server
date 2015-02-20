package Work

type AuctionDataResult struct {
	Result
	ResponseFailed bool
	AlreadyChecked bool
}

func (self AuctionDataResult) CanContinue() bool {
	return !self.AlreadyChecked && !self.ResponseFailed
}
