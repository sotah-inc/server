package state

import (
	"encoding/json"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/blizzard"
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

		//hellRegionRealms, err := sta.IO.HellClient.GetRegionRealms(regionRealmSlugs, gameversions.Retail)
		//if err != nil {
		//	m.Err = err.Error()
		//	m.Code = mCodes.GenericError
		//	sta.IO.Messenger.ReplyTo(natsMsg, m)
		//
		//	return
		//}

		//for regionName, hellRealms := range hellRegionRealms {
		//	nextHellRealms := func() hell.RealmsMap {
		//		foundHellRealms, ok := sta.HellRegionRealms[regionName]
		//		if !ok {
		//			return hell.RealmsMap{}
		//		}
		//
		//		return foundHellRealms
		//	}()
		//
		//	for realmSlug, hellRealm := range hellRealms {
		//		nextHellRealms[realmSlug] = hellRealm
		//	}
		//
		//	sta.HellRegionRealms[regionName] = nextHellRealms
		//}

		sta.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
