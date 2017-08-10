package app

import (
	"encoding/json"
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

func newMessenger(host string, port int) (messenger, error) {
	conn, err := nats.Connect(fmt.Sprintf("nats://%s:%d", host, port))
	if err != nil {
		return messenger{}, err
	}

	mess := messenger{conn: conn}

	return mess, nil
}

func (mess messenger) handleError(natsMsg *nats.Msg, err error) {
	// catching the error for publishing
	msg := message{Data: "", Err: err.Error()}
	body, err := json.Marshal(msg)

	// error on creating an error message should never happen so let's panic
	if err != nil {
		panic(err.Error()) // MASS HYSTERIA
	}

	// publishing out the error message
	mess.conn.Publish(natsMsg.Reply, body)
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

func (mess messenger) listenForStatus(stop chan interface{}) error {
	err := mess.subscribe(subjects.Status, stop, func(natsMsg *nats.Msg) {
		encodedStatus, err := json.Marshal(mess.status)
		if err != nil {
			mess.handleError(natsMsg, err)
		}

		mess.conn.Publish(natsMsg.Reply, encodedStatus)
	})
	if err != nil {
		return err
	}

	return nil
}
