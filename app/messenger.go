package app

import (
	"fmt"
	"time"

	"github.com/ihsw/go-download/app/subjects"

	"github.com/nats-io/go-nats"
)

type messenger struct {
	enConn *nats.EncodedConn
	status *status
}

func newMessenger(host string, port int) (messenger, error) {
	conn, err := nats.Connect(fmt.Sprintf("nats://%s:%d", host, port))
	if err != nil {
		return messenger{}, err
	}

	enConn, err := nats.NewEncodedConn(conn, nats.JSON_ENCODER)
	if err != nil {
		return messenger{}, err
	}

	mess := messenger{enConn: enConn}

	return mess, nil
}

func (mess messenger) statusListen() (*nats.Subscription, error) {
	return mess.enConn.Subscribe(subjects.Status, func(subject, reply string, msg interface{}) {
		mess.enConn.Publish(reply, mess.status)
	})
}

func (mess messenger) requestStatus() (*status, error) {
	sta := &status{}
	err := mess.enConn.Request(subjects.Status, struct{}{}, sta, 5*time.Second)
	if err != nil {
		return nil, err
	}

	return sta, nil
}
