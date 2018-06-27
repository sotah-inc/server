package blizzard

import (
	"testing"

	"github.com/ihsw/sotah-server/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestNewRealmFromFilepath(t *testing.T) {
	_, err := NewRealmFromFilepath("../TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}
}

func TestNewRealm(t *testing.T) {
	body, err := utiltest.ReadFile("../TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	_, err = newRealm(body)
	if !assert.Nil(t, err) {
		return
	}
}
