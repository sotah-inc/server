package Character

import (
	"encoding/json"
	"github.com/ihsw/go-download/Entity"
)

type Character struct {
	Id    int64
	Name  string
	Realm Entity.Realm
}

func (self Character) marshal() (string, error) {
	characterJson := CharacterJson{
		Id:      self.Id,
		Name:    self.Name,
		RealmId: self.Realm.Id,
	}

	return characterJson.marshal()
}

/*
	CharacterJson
*/
type CharacterJson struct {
	Id      int64  `json:"0"`
	Name    string `json:"1"`
	RealmId int64  `json:"2"`
}

func (self CharacterJson) marshal() (string, error) {
	b, err := json.Marshal(self)
	return string(b), err
}
