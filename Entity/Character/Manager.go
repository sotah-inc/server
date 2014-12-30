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

func (self Manager) PersistAll(realm Entity.Realm, existingCharacters []Character, newCharacters []Character) (characters []Character, err error) {
	var (
		ids []int64
		s   string
	)
	m := self.Client.Main

	if len(existingCharacters) > 0 {
		err = errors.New(fmt.Sprintf("%d existing characters found", len(existingCharacters)))
		return characters, err
	}

	// ids
	ids, err = m.IncrAll(fmt.Sprintf("realm:%d:character_id", realm.Id), len(newCharacters))
	if err != nil {
		return characters, err
	}
	for i, id := range ids {
		newCharacters[i].Id = id
	}

	// data
	values := make([]Cache.PersistValue, len(newCharacters))
	for i, character := range newCharacters {
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
	newCharacterIds := make([]string, len(newCharacters))
	newNames := make([]string, len(newCharacters))
	for i, character := range newCharacters {
		newCharacterIds[i] = strconv.FormatInt(character.Id, 10)
		newNames[i] = character.Name
	}
	err = m.RPushAll(fmt.Sprintf("realm:%d:character_ids", realm.Id), newCharacterIds)
	if err != nil {
		return characters, err
	}
	err = m.SAddAll(fmt.Sprintf("realm:%d:character_names", realm.Id), newNames)
	if err != nil {
		return characters, err
	}

	// merging them together
	characters = make([]Character, len(existingCharacters)+len(newCharacters))
	i := 0
	for _, character := range existingCharacters {
		characters[i] = character
		i++
	}
	for _, character := range newCharacters {
		characters[i] = character
		i++
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
