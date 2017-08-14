package app

import (
	"encoding/json"

	"github.com/ihsw/go-download/app/subjects"
	nats "github.com/nats-io/go-nats"
)

type state struct {
	messenger messenger
	status    *status
	auctions  map[regionName]map[realmSlug]auctions
}

func (sta state) listenForStatus(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Status, stop, func(natsMsg *nats.Msg) {
		m := message{}

		encodedStatus, err := json.Marshal(sta.status)
		if err != nil {
			m.Err = err.Error()
		} else {
			m.Data = string(encodedStatus)
		}

		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
