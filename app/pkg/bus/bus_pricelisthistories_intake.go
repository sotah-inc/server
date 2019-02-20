package bus

import (
	"encoding/json"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
)

type LoadRegionRealmTimestampsInJob struct {
	RegionName      string `json:"region_name"`
	RealmSlug       string `json:"realm_slug"`
	TargetTimestamp int    `json:"target_timestamp"`
}

func (c Client) LoadRegionRealmTimestamps(rTimestamps sotah.RegionRealmTimestamps) {
	// establishing channels for intake
	in := make(chan LoadRegionRealmTimestampsInJob)

	// spinning up the workers
	worker := func() {
		for inJob := range in {
			jsonEncoded, err := json.Marshal(inJob)
			if err != nil {
				logging.WithField("error", err.Error()).Fatal("Failed to encode realm")

				return
			}

			msg := NewMessage()
			msg.Data = string(jsonEncoded)

			logging.WithFields(logrus.Fields{
				"region": inJob.RegionName,
				"realm":  inJob.RealmSlug,
			}).Info("Queueing up realm")
			if _, err := c.Publish(c.Topic(string(subjects.PricelistHistoriesCompute)), msg); err != nil {
				logging.WithField("error", err.Error()).Fatal("Failed to publish message")

				return
			}
		}
	}
	postWork := func() {
		return
	}
	util.Work(16, worker, postWork)

	// queueing up the realms
	go func() {
		logging.Info("Queueing up realms")
		for regionName, realmTimestamps := range rTimestamps {
			for realmSlug, targetTime := range realmTimestamps {
				in <- LoadRegionRealmTimestampsInJob{
					RegionName:      string(regionName),
					RealmSlug:       string(realmSlug),
					TargetTimestamp: int(targetTime),
				}
			}

			break
		}

		close(in)
	}()
}
