package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConfig(t *testing.T) {
	c, err := newConfig("./TestData/config.json")
	if !assert.Nil(t, err) {
		return
	}

	if !assert.NotEmpty(t, c.APIKey) {
		return
	}
}
