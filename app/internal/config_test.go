package internal

import (
	"testing"

	"github.com/sotah-inc/server/app/pkg/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestNewConfigFromFilepath(t *testing.T) {
	c, err := NewConfigFromFilepath("./TestData/Config.json")
	if !assert.Nil(t, err) || !assert.NotEmpty(t, c.ClientID) {
		return
	}
}
func TestNewConfig(t *testing.T) {
	body, err := utiltest.ReadFile("./TestData/Config.json")
	if !assert.Nil(t, err) {
		return
	}

	c, err := newConfig(body)
	if !assert.Nil(t, err) || !assert.NotEmpty(t, c.ClientID) {
		return
	}
}
func TestGetStatus(t *testing.T) {
	c, err := NewConfigFromFilepath("./TestData/Config.json")
	if !assert.Nil(t, err) || !assert.NotEmpty(t, c.Regions) {
		return
	}

	reg := c.Regions[0]

	realmStatusTs, err := utiltest.ServeFile("./TestData/Realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	s, err := reg.GetStatus(
		Resolver{GetStatusURL: func(regionHostname string) string { return realmStatusTs.URL }},
	)
	if !assert.Nil(t, err) || !assert.NotEmpty(t, s.Realms) {
		return
	}
}
