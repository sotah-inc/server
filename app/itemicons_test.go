package main

import (
	"testing"

	"github.com/ihsw/sotah-server/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestSyncItemIcon(t *testing.T) {
	ts, err := utiltest.ServeFile("./TestData/inv_sword_04.jpg")
	if !assert.Nil(t, err) {
		return
	}

	err = syncItemIcon("inv_sword_04", resolver{
		getItemIconURL: func(name string) string { return ts.URL },
		config:         &config{},
	})
	if !assert.Nil(t, err) {
		return
	}
}
