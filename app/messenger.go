package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
)

type messenger struct {
	conn *nats.Conn
}

func newMessage() message {
	return message{Code: codes.Ok}
}

type message struct {
	Data string     `json:"data"`
	Err  string     `json:"error"`
	Code codes.Code `json:"code"`
}

func (m message) parse() ([]byte, error) {
	if len(m.Err) > 0 {
		return []byte{}, errors.New(m.Err)
	}

	return []byte(m.Data), nil
}

func newMessengerFromEnvVars(hostKey string, portKey string) (messenger, error) {
	natsHost := os.Getenv(hostKey)
	natsPort := os.Getenv(portKey)
	if len(natsPort) == 0 {
		return messenger{}, errors.New("Nats port cannot be blank")
	}

	parsedNatsPort, err := strconv.Atoi(natsPort)
	if err != nil {
		return messenger{}, err
	}

	return newMessenger(natsHost, parsedNatsPort)
}

func newMessenger(host string, port int) (messenger, error) {
	if len(host) == 0 {
		return messenger{}, errors.New("Host cannot be blank")
	}

	natsURI := fmt.Sprintf("nats://%s:%d", host, port)

	logging.WithField("uri", natsURI).Info("Connecting to nats")

	conn, err := nats.Connect(natsURI)
	if err != nil {
		return messenger{}, err
	}

	mess := messenger{conn: conn}

	return mess, nil
}

func (mess messenger) subscribe(subject subjects.Subject, stop listenStopChan, cb func(nats.Msg)) error {
	logging.WithField("subject", subject).Info("Subscribing to subject")

	sub, err := mess.conn.Subscribe(string(subject), func(natsMsg *nats.Msg) {
		logging.WithField("subject", subject).Debug("Received request")

		cb(*natsMsg)
	})
	if err != nil {
		return err
	}

	go func() {
		<-stop

		logging.WithField("subject", subject).Info("Unsubscribing from subject")

		sub.Unsubscribe()
	}()

	return nil
}

func (mess messenger) replyTo(natsMsg nats.Msg, m message) error {
	if m.Code == codes.Blank {
		return errors.New("Code cannot be blank")
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

	// attempting to publish it
	err = mess.conn.Publish(natsMsg.Reply, jsonMessage)
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":   err.Error(),
			"subject": natsMsg.Reply,
		}).Error("Failed to publish message")
		return err
	}

	return nil
}

func (mess messenger) request(subject subjects.Subject, data []byte) (message, error) {
	natsMsg, err := mess.conn.Request(string(subject), data, 5*time.Second)
	if err != nil {
		return message{}, err
	}

	// json-decoding the message
	msg := &message{}
	if err = json.Unmarshal(natsMsg.Data, &msg); err != nil {
		return message{}, err
	}

	return *msg, nil
}

func (mess messenger) publish(subject subjects.Subject, data []byte) error {
	return mess.conn.Publish(string(subject), data)
}

type telegrafMetrics map[string]int64

func (mess messenger) publishMetric(metrics telegrafMetrics) error {
	result, err := json.Marshal(metrics)
	if err != nil {
		return err
	}

	return mess.publish(subjects.AppMetrics, result)
}

func (mess messenger) publishPlanMetaMetric(resp blizzard.ResponseMeta) error {
	return mess.publishMetric(telegrafMetrics{
		"qps_allotted":        int64(resp.PlanQPSAllotted),
		"qps_current":         int64(resp.PlanQPSCurrent),
		"quota_allotted":      int64(resp.PlanQuotaAllotted),
		"quota_current":       int64(resp.PlanQuotaCurrent),
		"ingress_body_length": int64(len(resp.Body)),
	})
}

func (mess messenger) publishBodyIngressMetric(bodyLength int) error {
	return mess.publishMetric(telegrafMetrics{"ingress_body_length": int64(bodyLength)})
}
