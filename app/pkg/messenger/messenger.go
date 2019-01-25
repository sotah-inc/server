package messenger

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
)

type Messenger struct {
	conn *nats.Conn
}

func NewMessage() Message {
	return Message{Code: codes.Ok}
}

type Message struct {
	Data string     `json:"data"`
	Err  string     `json:"error"`
	Code codes.Code `json:"code"`
}

func (m Message) parse() ([]byte, error) {
	if len(m.Err) > 0 {
		return []byte{}, errors.New(m.Err)
	}

	return []byte(m.Data), nil
}

func NewMessengerFromEnvVars(hostKey string, portKey string) (Messenger, error) {
	natsHost := os.Getenv(hostKey)
	natsPort := os.Getenv(portKey)
	if len(natsPort) == 0 {
		return Messenger{}, errors.New("nats port cannot be blank")
	}

	parsedNatsPort, err := strconv.Atoi(natsPort)
	if err != nil {
		return Messenger{}, err
	}

	return NewMessenger(natsHost, parsedNatsPort)
}

func NewMessenger(host string, port int) (Messenger, error) {
	if len(host) == 0 {
		return Messenger{}, errors.New("host cannot be blank")
	}

	natsURI := fmt.Sprintf("nats://%s:%d", host, port)

	logging.WithField("uri", natsURI).Info("Connecting to nats")

	conn, err := nats.Connect(natsURI)
	if err != nil {
		return Messenger{}, err
	}

	mess := Messenger{conn: conn}

	return mess, nil
}

type ListenStopChan chan interface{}

func (mess Messenger) Subscribe(subject subjects.Subject, stop ListenStopChan, cb func(nats.Msg)) error {
	logging.WithField("subject", subject).Debug("Subscribing to subject")

	sub, err := mess.conn.Subscribe(string(subject), func(natsMsg *nats.Msg) {
		logging.WithField("subject", subject).Debug("Received Request")

		cb(*natsMsg)
	})
	if err != nil {
		return err
	}

	go func() {
		<-stop

		logging.WithField("subject", subject).Info("Unsubscribing from subject")

		if err := sub.Unsubscribe(); err != nil {
			logging.WithField("error", err.Error()).Error("failed to unsubscribe")

			return
		}
	}()

	return nil
}

func (mess Messenger) ReplyTo(natsMsg nats.Msg, m Message) error {
	if m.Code == codes.Blank {
		return errors.New("code cannot be blank")
	}

	// json-encoding the message
	jsonMessage, err := json.Marshal(m)
	if err != nil {
		return err
	}

	if m.Code != codes.Ok {
		logging.WithFields(logrus.Fields{
			"error":          m.Err,
			"code":           m.Code,
			"reply_to":       natsMsg.Reply,
			"payload_length": len(jsonMessage),
		}).Error("Publishing an erroneous reply")
	} else {
		logging.WithFields(logrus.Fields{
			"reply_to":       natsMsg.Reply,
			"payload_length": len(jsonMessage),
			"code":           m.Code,
		}).Debug("Publishing a reply")
	}

	// attempting to Publish it
	err = mess.conn.Publish(natsMsg.Reply, jsonMessage)
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":   err.Error(),
			"subject": natsMsg.Reply,
		}).Error("Failed to Publish message")

		return err
	}

	return nil
}

func (mess Messenger) Request(subject subjects.Subject, data []byte) (Message, error) {
	natsMsg, err := mess.conn.Request(string(subject), data, 5*time.Second)
	if err != nil {
		return Message{}, err
	}

	// json-decoding the message
	msg := &Message{}
	if err = json.Unmarshal(natsMsg.Data, &msg); err != nil {
		return Message{}, err
	}

	return *msg, nil
}

func (mess Messenger) Publish(subject subjects.Subject, data []byte) error {
	return mess.conn.Publish(string(subject), data)
}
