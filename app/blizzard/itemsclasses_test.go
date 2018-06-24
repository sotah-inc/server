package blizzard

import (
	"testing"

	"github.com/ihsw/sotah-server/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func validateItemClasses(iClasses itemClasses) bool {
	return len(iClasses.Classes) > 0 && iClasses.Classes[0].Class != 0
}

func TestNewItemClassesFromHTTP(t *testing.T) {
	ts, err := utiltest.ServeFile("../TestData/item-classes.json")
	if !assert.Nil(t, err) {
		return
	}

	a, err := newItemClassesFromHTTP(ts.URL)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.True(t, validateItemClasses(a)) {
		return
	}
}

func TestNewItemClassesFromFilepath(t *testing.T) {
	iClasses, err := newItemClassesFromFilepath("../TestData/item-classes.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.True(t, validateItemClasses(iClasses)) {
		return
	}
}
