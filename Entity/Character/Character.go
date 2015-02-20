package Character

import (
	"encoding/json"
	"github.com/ihsw/go-download/Entity"
)

/*
	Character
*/
type Character struct {
	Id        int64
	Name      string
	Realm     Entity.Realm
	GuildName string
}

func (self Character) marshal() (string, error) {
	characterJson := CharacterJson{
		Id:        self.Id,
		Name:      self.Name,
		RealmId:   self.Realm.Id,
		GuildName: self.GuildName,
	}

	return characterJson.marshal()
}

func (self Character) IsValid() bool { return self.Id != 0 }

/*
	CharacterJson
*/
type CharacterJson struct {
	Id        int64  `json:"0"`
	Name      string `json:"1"`
	RealmId   int64  `json:"2"`
	GuildName string `json:"3"`
}

func (self CharacterJson) marshal() (string, error) {
	b, err := json.Marshal(self)
	return string(b), err
}
