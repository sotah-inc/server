package app

import (
	"testing"

	"github.com/ihsw/go-download/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestNewRegion(t *testing.T) {
	c, err := newConfig("./TestData/config.json")
	if !assert.Nil(t, err) {
		return
	}

	if !assert.NotEmpty(t, c.Regions) {
		return
	}

	reg := newRegion(c.Regions[0])
	if !assert.NotEmpty(t, reg.name) {
		return
	}
}
func TestGetStatus(t *testing.T) {
	c, err := newConfig("./TestData/config.json")
	if !assert.Nil(t, err) {
		return
	}

	if !assert.NotEmpty(t, c.Regions) {
		return
	}

	reg := newRegion(c.Regions[0])
	if !assert.NotEmpty(t, reg.hostname) {
		return
	}

	realmStatusTs, err := utiltest.ServeFile("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	s, err := reg.getStatus(
		resolver{getStatusURL: func(regionHostname string) string { return realmStatusTs.URL }},
	)
	if !assert.NotEmpty(t, s.Realms) {
		return
	}
}
