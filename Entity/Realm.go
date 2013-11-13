package Entity

import (
	"time"
)

/*
	Realm
*/
type Realm struct {
	Id             int64
	Name           string
	Slug           string
	Battlegroup    string
	Type           string
	Status         bool
	LastChecked    time.Time
	Region         Region
	Population     string
	LastDownloaded time.Time
}

type StatusRealm interface {
	BattleGroup() string
	Name() string
	Population() string
	Slug() string
	Status() bool
	RealmType() string
}
