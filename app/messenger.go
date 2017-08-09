package app

import (
	"fmt"

	"github.com/nats-io/go-nats"
)

type messenger struct {
	client *nats.Conn
}

func newMessenger(host string, port int) (messenger, error) {
	conn, err := nats.Connect(fmt.Sprintf("nats://%s:%d", host, port))
	if err != nil {
		return messenger{}, err
	}

	mess := messenger{client: conn}

	return mess, nil
}
