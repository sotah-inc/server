package Work

import (
	"github.com/ihsw/go-download/Entity"
)

type Result struct {
	responseFailed bool
	alreadyChecked bool
	err            error
	realm          Entity.Realm
}

func (self Result) CanContinue() bool {
	return !self.alreadyChecked && !self.responseFailed
}
