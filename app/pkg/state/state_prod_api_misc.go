package state

import (
	"encoding/json"
	"errors"

	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

type AuthenticatedBootResponse struct {
	BootResponse

	BlizzardClientId     string `json:"blizzard_client_id"`
	BlizzardClientSecret string `json:"blizzard_client_secret"`
}

func (sta ProdApiState) ListenForAuthenticatedBoot(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
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
				reply.Code = codes.MsgJSONParseError
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

func (sta ProdApiState) ListenForStatus(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			reply := bus.NewMessage()

			sr, err := newStatusRequest([]byte(busMsg.Data))
			if err != nil {
				reply.Err = err.Error()
				reply.Code = codes.MsgJSONParseError
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
				reply.Code = codes.NotFound
				if _, err := sta.IO.BusClient.ReplyTo(busMsg, reply); err != nil {
					logging.WithField("error", err.Error()).Error("Failed to reply")

					return
				}

				return
			}

			regionStatus, ok := sta.Statuses[reg.Name]
			if !ok {
				reply.Err = "Region found but not in Statuses"
				reply.Code = codes.NotFound
				if _, err := sta.IO.BusClient.ReplyTo(busMsg, reply); err != nil {
					logging.WithField("error", err.Error()).Error("Failed to reply")

					return
				}

				return
			}

			encodedStatus, err := json.Marshal(regionStatus)
			if err != nil {
				reply.Err = err.Error()
				reply.Code = codes.GenericError
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
