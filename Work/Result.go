package Work

import (
	"github.com/ihsw/go-download/Entity"
)

type Result struct {
	pass           bool
	alreadyChecked bool
	err            error
	realm          Entity.Realm
}
