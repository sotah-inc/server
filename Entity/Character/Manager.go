package Character

import (
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
