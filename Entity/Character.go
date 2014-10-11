package Entity

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"strconv"
)

/*
	Character
*/
type Character struct {
	Id    int64
	Name  string
	Realm Realm
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

/*
	CharacterManager
*/
type CharacterManager struct {
	Client Cache.Client
}

func (self CharacterManager) Namespace() string { return "character" }

func (self CharacterManager) PersistAll(realm Realm, characters []Character) ([]Character, error) {
	var (
		err error
		ids []int64
		s   string
	)
	m := self.Client.Main

	// ids
	ids, err = m.IncrAll(fmt.Sprintf("realm:%d:character_id", realm.Id), len(characters))
	if err != nil {
		return characters, err
	}
	for i, id := range ids {
		characters[i].Id = id
	}

	// data
	values := make([]Cache.PersistValue, len(characters))
	for i, character := range characters {
		s, err = character.marshal()
		bucketKey, subKey := Cache.GetBucketKey(character.Id, self.Namespace())
		values[i] = Cache.PersistValue{
			BucketKey: bucketKey,
			SubKey:    subKey,
			Value:     s,
		}
	}
	err = m.PersistAll(values)
	if err != nil {
		return characters, err
	}

	// etc
	characterIds := make([]string, len(characters))
	for i, character := range characters {
		characterIds[i] = strconv.FormatInt(character.Id, 10)
	}
	err = m.RPushAll(fmt.Sprintf("realm:%d:character_ids", realm.Id), characterIds)
	if err != nil {
		return characters, err
	}

	return characters, nil
}
