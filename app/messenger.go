package app

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ihsw/go-download/app/subjects"
	"github.com/nats-io/go-nats"
)

type messenger struct {
	conn     *nats.Conn
	status   *status
	auctions map[regionName]map[realmSlug]auctions
}

type message struct {
	Data string `json:"data"`
	Err  string `json:"error"`
}

func (m message) parse() ([]byte, error) {
	if len(m.Err) > 0 {
		return []byte{}, errors.New(m.Err)
	}

	return []byte(m.Data), nil
}

func newMessenger(host string, port int) (messenger, error) {
	conn, err := nats.Connect(fmt.Sprintf("nats://%s:%d", host, port))
	if err != nil {
		return messenger{}, err
	}

	mess := messenger{conn: conn}

	return mess, nil
}

func (mess messenger) subscribe(subject string, stop chan interface{}, cb func(*nats.Msg)) error {
	sub, err := mess.conn.Subscribe(subject, cb)
	if err != nil {
		return err
	}

	go func() {
		<-stop
		sub.Unsubscribe()
	}()

	return nil
}

func (mess messenger) publish(subject string, m message) error {
	encodedMessage, err := json.Marshal(m)
	if err != nil {
		return err
	}
	mess.conn.Publish(subject, encodedMessage)

	return nil
}

func (mess messenger) listenForStatus(stop chan interface{}) error {
	err := mess.subscribe(subjects.Status, stop, func(natsMsg *nats.Msg) {
		m := message{}

		encodedStatus, err := json.Marshal(mess.status)
		if err != nil {
			m.Err = err.Error()
		} else {
			m.Data = string(encodedStatus)
		}

		mess.publish(natsMsg.Reply, m)
	})
	if err != nil {
		return err
	}

	return nil
}
