package app

import (
	"encoding/json"
	"fmt"

	"github.com/ihsw/go-download/app/subjects"
	"github.com/nats-io/go-nats"
)

type messenger struct {
	conn   *nats.Conn
	status *status
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

func (mess messenger) listenForStatus(stop chan interface{}) error {
	ch := make(chan *nats.Msg, 64)
	sub, err := mess.conn.ChanSubscribe(subjects.Status, ch)
	if err != nil {
		return err
	}

	go func() {
		for natsMsg := range ch {
			// encoding the status
			encodedStatus, err := json.Marshal(mess.status)
			if err != nil {
				// catching the error for publishing
				msg := message{Data: "", Err: err.Error()}
				body, err := json.Marshal(msg)

				// error on creating an error message should never happen so let's panic
				if err != nil {
					panic(err.Error()) // MASS HYSTERIA
				}

				// publishing out the error message
				mess.conn.Publish(natsMsg.Reply, body)

				continue
			}

			// publishing the status
			mess.conn.Publish(natsMsg.Reply, encodedStatus)
		}
	}()

	go func() {
		<-stop
		close(ch)
		sub.Unsubscribe()
	}()

	return nil
}
