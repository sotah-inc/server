package state

import (
	"encoding/json"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/bus"
	bCodes "github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func (sta ProdApiState) ListenForMessengerBoot(stop ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(string(subjects.Boot), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		encodedResponse, err := json.Marshal(BootResponse{
			Regions:     sta.Regions,
			ItemClasses: sta.ItemClasses,
			Expansions:  sta.Expansions,
			Professions: sta.Professions,
		})
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.MsgJSONParseError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedResponse)
		sta.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

type AuthenticatedBootResponse struct {
	BootResponse

	BlizzardClientId     string `json:"blizzard_client_id"`
	BlizzardClientSecret string `json:"blizzard_client_secret"`
}

func (sta ProdApiState) ListenForBusAuthenticatedBoot(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			reply := bus.NewMessage()

			encodedResponse, err := json.Marshal(AuthenticatedBootResponse{
				BootResponse: BootResponse{
					Regions:     sta.Regions,
					ItemClasses: sta.ItemClasses,
					Expansions:  sta.Expansions,
					Professions: sta.Professions,
				},
				BlizzardClientId:     sta.BlizzardClientId,
				BlizzardClientSecret: sta.BlizzardClientSecret,
			})
			if err != nil {
				reply.Err = err.Error()
				reply.Code = bCodes.MsgJSONParseError
				sta.IO.BusClient.ReplyTo(busMsg, reply)

				return
			}

			reply.Data = string(encodedResponse)
			sta.IO.BusClient.ReplyTo(busMsg, reply)
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := sta.IO.BusClient.SubscribeToTopic(string(subjects.Boot), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
