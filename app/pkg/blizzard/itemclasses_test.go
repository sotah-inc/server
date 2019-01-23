package blizzard

import (
	"testing"

	"github.com/sotah-inc/server/app/pkg/utiltest"
	"github.com/stretchr/testify/assert"
)

func validateItemClasses(iClasses ItemClasses) bool {
	return len(iClasses.Classes) > 0 && iClasses.Classes[0].Class != 0
}

func TestNewItemClassesFromHTTP(t *testing.T) {
	ts, err := utiltest.ServeFile("../TestData/item-classes.json")
	if !assert.Nil(t, err) {
		return
	}

	a, _, err := NewItemClassesFromHTTP(ts.URL)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.True(t, validateItemClasses(a)) {
		return
	}
}

func TestNewItemClassesFromFilepath(t *testing.T) {
	iClasses, err := NewItemClassesFromFilepath("../TestData/item-classes.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.True(t, validateItemClasses(iClasses)) {
		return
	}
}
