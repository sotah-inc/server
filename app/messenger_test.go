package main

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMessengerFromEnvVars(t *testing.T) {
	_, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
}

func TestNewMessenger(t *testing.T) {
	natsHost := os.Getenv("NATS_HOST")
	if !assert.NotEmpty(t, natsHost) {
		return
	}
	natsPort, err := strconv.Atoi(os.Getenv("NATS_PORT"))
	if !assert.Nil(t, err) || !assert.NotEmpty(t, natsPort) {
		return
	}

	_, err = newMessenger(natsHost, natsPort)
	if !assert.Nil(t, err) {
		return
	}
}
