package main

import (
	"testing"

	"github.com/sotah-inc/server/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestNewConfigFromFilepath(t *testing.T) {
	c, err := newConfigFromFilepath("./TestData/config.json")
	if !assert.Nil(t, err) || !assert.NotEmpty(t, c.APIKey) {
		return
	}
}
func TestNewConfig(t *testing.T) {
	body, err := utiltest.ReadFile("./TestData/config.json")
	if !assert.Nil(t, err) {
		return
	}

	c, err := newConfig(body)
	if !assert.Nil(t, err) || !assert.NotEmpty(t, c.APIKey) {
		return
	}
}
func TestGetStatus(t *testing.T) {
	c, err := newConfigFromFilepath("./TestData/config.json")
	if !assert.Nil(t, err) || !assert.NotEmpty(t, c.Regions) {
		return
	}

	reg := c.Regions[0]

	realmStatusTs, err := utiltest.ServeFile("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	s, err := reg.getStatus(
		resolver{getStatusURL: func(regionHostname string) string { return realmStatusTs.URL }},
	)
	if !assert.Nil(t, err) || !assert.NotEmpty(t, s.Realms) {
		return
	}
}
