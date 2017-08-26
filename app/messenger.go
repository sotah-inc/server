package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/nats-io/go-nats"
)

type messenger struct {
	conn *nats.Conn
}

func newMessage() message {
	return message{Code: codes.Ok}
}

type message struct {
	Data string `json:"data"`
	Err  string `json:"error"`
	Code int    `json:"code"`
}

func (m message) parse() ([]byte, error) {
	if len(m.Err) > 0 {
		return []byte{}, errors.New(m.Err)
	}

	return []byte(m.Data), nil
}

func newMessengerFromEnvVars(hostKey string, portKey string) (messenger, error) {
	natsHost := os.Getenv(hostKey)
	natsPort, err := strconv.Atoi(os.Getenv(portKey))
	if err != nil {
		return messenger{}, err
	}

	return newMessenger(natsHost, natsPort)
}

func newMessenger(host string, port int) (messenger, error) {
	if len(host) == 0 {
		return messenger{}, errors.New("Host cannot be blank")
	}

	natsURI := fmt.Sprintf("nats://%s:%d", host, port)

	log.WithField("uri", natsURI).Info("Connecting to nats")

	conn, err := nats.Connect(natsURI)
	if err != nil {
		return messenger{}, err
	}

	mess := messenger{conn: conn}

	return mess, nil
}

func (mess messenger) subscribe(subject string, stop chan interface{}, cb func(*nats.Msg)) error {
	log.WithField("subject", subject).Info("Subscribing to subject")

	sub, err := mess.conn.Subscribe(subject, func(natsMsg *nats.Msg) {
		log.WithField("subject", subject).Debug("Received request")

		cb(natsMsg)
	})
	if err != nil {
		return err
	}

	go func() {
		<-stop

		log.WithField("subject", subject).Info("Unsubscribing from subject")

		sub.Unsubscribe()
	}()

	return nil
}

func (mess messenger) replyTo(natsMsg *nats.Msg, m message) error {
	if m.Code == codes.Blank {
		return errors.New("Code cannot be blank")
	}

	encodedMessage, err := json.Marshal(m)
	if err != nil {
		return err
	}

	log.WithField("subject", natsMsg.Reply).Debug("Publishing reply")

	mess.conn.Publish(natsMsg.Reply, encodedMessage)

	return nil
}

func (mess messenger) request(subject string, data []byte) ([]byte, error) {
	log.WithField("subject", subject).Debug("Requesting message")

	natsMsg, err := mess.conn.Request(subject, data, 5*time.Second)
	if err != nil {
		return []byte{}, err
	}

	msg := &message{}
	if err = json.Unmarshal(natsMsg.Data, &msg); err != nil {
		return []byte{}, err
	}

	return msg.parse()
}
