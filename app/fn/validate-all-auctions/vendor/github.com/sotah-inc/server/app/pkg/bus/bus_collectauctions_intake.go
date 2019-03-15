package bus

import (
	"encoding/json"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
)

type LoadRegionRealmsOutJob struct {
	Err   error
	Realm sotah.Realm
}

func (c Client) LoadRegionRealms(recipientTopic *pubsub.Topic, regionRealms map[blizzard.RegionName]sotah.Realms) chan LoadRegionRealmsOutJob {
	// establishing channels for intake
	in := make(chan sotah.Realm)
	out := make(chan LoadRegionRealmsOutJob)

	// spinning up the workers
	worker := func() {
		for realm := range in {
			job := CollectAuctionsJob{
				RegionName: string(realm.Region.Name),
				RealmSlug:  string(realm.Slug),
			}
			jsonEncoded, err := json.Marshal(job)
			if err != nil {
				out <- LoadRegionRealmsOutJob{
					Err:   err,
					Realm: realm,
				}

				return
			}

			msg := NewMessage()
			msg.Data = string(jsonEncoded)
			if _, err := c.Publish(recipientTopic, msg); err != nil {
				out <- LoadRegionRealmsOutJob{
					Err:   err,
					Realm: realm,
				}

				return
			}

			out <- LoadRegionRealmsOutJob{
				Err:   nil,
				Realm: realm,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(16, worker, postWork)

	// queueing up the realms
	go func() {
		logging.Info("Queueing up realms")
		for _, realms := range regionRealms {
			for _, realm := range realms {
				in <- realm
			}
		}

		close(in)
	}()

	return out
}
