package Work

import (
	"github.com/ihsw/go-download/Entity"
)

type Result struct {
	ResponseFailed bool
	AlreadyChecked bool
	Err            error
	realm          Entity.Realm
}

func (self Result) CanContinue() bool {
	return !self.AlreadyChecked && !self.ResponseFailed
}
