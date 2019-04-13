package state

import (
	"encoding/json"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"

	"github.com/sotah-inc/server/app/pkg/blizzard"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/messenger"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func (sta ProdApiState) ListenForReceiveRealms(stop ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(string(subjects.ReceiveRealms), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		var regionRealmSlugs map[blizzard.RegionName][]blizzard.RealmSlug
		if err := json.Unmarshal(natsMsg.Data, &regionRealmSlugs); err != nil {
			m.Err = err.Error()
			m.Code = mCodes.GenericError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		for regionName, realmSlugs := range regionRealmSlugs {
			realmSlugWhitelist := map[blizzard.RealmSlug]interface{}{}
			for _, realmSlug := range realmSlugs {
				realmSlugWhitelist[realmSlug] = struct{}{}
			}

			realms, err := sta.RealmsBase.GetRealms(regionName, realmSlugWhitelist, sta.RealmsBucket)
			if err != nil {
				m.Err = err.Error()
				m.Code = mCodes.GenericError
				sta.IO.Messenger.ReplyTo(natsMsg, m)

				return
			}

			logging.WithFields(logrus.Fields{
				"region": regionName,
				"realms": len(realms),
			}).Info("Received realms")
			status := sta.Statuses[regionName]
			status.Realms = realms
			sta.Statuses[regionName] = status
		}

		sta.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
