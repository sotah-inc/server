package Character

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"strconv"
)

type Manager struct {
	Client Cache.Client
}

func (self Manager) Namespace() string { return "character" }

func (self Manager) PersistAll(realm Entity.Realm, characters []Character) ([]Character, error) {
	var (
		err error
		ids []int64
		s   string
	)
	m := self.Client.Main

	for i, character := range characters {
		if character.IsValid() {
			err = errors.New(fmt.Sprintf("Character %s at %d is valid with id %d!", character.Name, i, character.Id))
			return characters, err
		}
	}

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
	names := make([]string, len(characters))
	for i, character := range characters {
		characterIds[i] = strconv.FormatInt(character.Id, 10)
		names[i] = character.Name
	}
	err = m.RPushAll(fmt.Sprintf("realm:%d:character_ids", realm.Id), characterIds)
	if err != nil {
		return characters, err
	}
	err = m.SAddAll(fmt.Sprintf("realm:%d:character_names", realm.Id), names)
	if err != nil {
		return characters, err
	}

	return characters, nil
}

func (self Manager) unmarshal(v string) (character Character, err error) {
	if v == "" {
		return
	}

	// json
	var characterJson CharacterJson
	b := []byte(v)
	err = json.Unmarshal(b, &characterJson)
	if err != nil {
		return
	}

	// initial
	character = Character{
		Id:   characterJson.Id,
		Name: characterJson.Name,
	}

	// resolving the realm
	realmManager := Entity.RealmManager{Client: self.Client}
	realm, err := realmManager.FindOneById(characterJson.RealmId)
	if err != nil {
		return
	}
	if !realm.IsValid() {
		err = errors.New(fmt.Sprintf("Realm #%d could not be found!", characterJson.RealmId))
		return
	}
	character.Realm = realm

	return character, nil
}

func (self Manager) unmarshalAll(values []string) (characters []Character, err error) {
	characters = make([]Character, len(values))
	for i, v := range values {
		characters[i], err = self.unmarshal(v)
		if err != nil {
			return
		}
	}
	return
}

func (self Manager) FindByRealm(realm Entity.Realm) (characters []Character, err error) {
	main := self.Client.Main

	// fetching ids
	ids, err := main.FetchIds(fmt.Sprintf("realm:%d:character_ids", realm.Id), 0, -1)
	if err != nil {
		return
	}

	// fetching the values
	var values []string
	values, err = main.FetchFromIds(self, ids)
	if err != nil {
		return
	}

	return self.unmarshalAll(values)
}
