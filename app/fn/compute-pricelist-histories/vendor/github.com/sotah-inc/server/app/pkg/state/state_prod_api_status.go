package state

import (
	"encoding/json"
	"errors"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/bus"
	bCodes "github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func (sta ProdApiState) ListenForBusStatus(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			reply := bus.NewMessage()

			sr, err := newStatusRequest([]byte(busMsg.Data))
			if err != nil {
				reply.Err = err.Error()
				reply.Code = bCodes.MsgJSONParseError
				if _, err := sta.IO.BusClient.ReplyTo(busMsg, reply); err != nil {
					logging.WithField("error", err.Error()).Error("Failed to reply")

					return
				}

				return
			}

			reg, err := func() (sotah.Region, error) {
				for _, r := range sta.Regions {
					if r.Name == sr.RegionName {
						return r, nil
					}
				}

				return sotah.Region{}, errors.New("could not find region")
			}()
			if err != nil {
				reply.Err = err.Error()
				reply.Code = bCodes.NotFound
				if _, err := sta.IO.BusClient.ReplyTo(busMsg, reply); err != nil {
					logging.WithField("error", err.Error()).Error("Failed to reply")

					return
				}

				return
			}

			regionStatus, ok := sta.Statuses[reg.Name]
			if !ok {
				reply.Err = "Region found but not in Statuses"
				reply.Code = bCodes.NotFound
				if _, err := sta.IO.BusClient.ReplyTo(busMsg, reply); err != nil {
					logging.WithField("error", err.Error()).Error("Failed to reply")

					return
				}

				return
			}

			encodedStatus, err := json.Marshal(regionStatus)
			if err != nil {
				reply.Err = err.Error()
				reply.Code = bCodes.GenericError
				if _, err := sta.IO.BusClient.ReplyTo(busMsg, reply); err != nil {
					logging.WithField("error", err.Error()).Error("Failed to reply")

					return
				}

				return
			}

			reply.Data = string(encodedStatus)
			if _, err := sta.IO.BusClient.ReplyTo(busMsg, reply); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to reply")

				return
			}
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := sta.IO.BusClient.SubscribeToTopic(string(subjects.Status), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}

func (sta ProdApiState) ListenForMessengerStatus(stop ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(string(subjects.Status), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		sr, err := newStatusRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.MsgJSONParseError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		reg, err := func() (sotah.Region, error) {
			for _, r := range sta.Regions {
				if r.Name == sr.RegionName {
					return r, nil
				}
			}

			return sotah.Region{}, errors.New("could not find region")
		}()
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.NotFound
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		regionStatus, ok := sta.Statuses[reg.Name]
		if !ok {
			m.Err = "Region found but not in Statuses"
			m.Code = mCodes.NotFound
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		encodedStatus, err := json.Marshal(regionStatus)
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.GenericError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedStatus)
		sta.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
