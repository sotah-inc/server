package util

import (
	"testing"

	"github.com/ihsw/sotah-server/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestGzipEncode(t *testing.T) {
	nonEncoded, err := utiltest.ReadFile("../TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	result, err := GzipEncode(nonEncoded)
	if !assert.Nil(t, err) {
		return
	}

	result, err = GzipDecode(result)
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Equal(t, nonEncoded, result) {
		return
	}
}

func TestGzipDecode(t *testing.T) {
	encoded, err := utiltest.ReadFile("../TestData/realm.json.gz")
	if !assert.Nil(t, err) {
		return
	}

	nonEncoded, err := utiltest.ReadFile("../TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	result, err := GzipDecode(encoded)
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Equal(t, nonEncoded, result) {
		return
	}
}
