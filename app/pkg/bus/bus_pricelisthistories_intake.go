package bus

import (
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
)

func NewLoadRegionRealmTimestampsInJob(data string) (LoadRegionRealmTimestampsInJob, error) {
	var out LoadRegionRealmTimestampsInJob
	if err := json.Unmarshal([]byte(data), &out); err != nil {
		return LoadRegionRealmTimestampsInJob{}, err
	}

	return out, nil
}

type LoadRegionRealmTimestampsInJob struct {
	RegionName      string `json:"region_name"`
	RealmSlug       string `json:"realm_slug"`
	TargetTimestamp int    `json:"target_timestamp"`
}

func (j LoadRegionRealmTimestampsInJob) EncodeForDelivery() (string, error) {
	out, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	return string(out), nil
}

func (j LoadRegionRealmTimestampsInJob) ToRegionRealmTimestampTuple() RegionRealmTimestampTuple {
	return RegionRealmTimestampTuple{
		RegionName:      string(j.RegionName),
		RealmSlug:       string(j.RealmSlug),
		TargetTimestamp: j.TargetTimestamp,
	}
}

func (j LoadRegionRealmTimestampsInJob) ToRegionRealmTime() (sotah.Region, sotah.Realm, time.Time) {
	region := sotah.Region{Name: blizzard.RegionName(j.RegionName)}
	realm := sotah.Realm{
		Realm:  blizzard.Realm{Slug: blizzard.RealmSlug(j.RealmSlug)},
		Region: region,
	}
	targetTime := time.Unix(int64(j.TargetTimestamp), 0)

	return region, realm, targetTime
}

func (c Client) LoadRegionRealmTimestamps(rTimestamps sotah.RegionRealmTimestampMaps, recipientSubject subjects.Subject) {
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
			if _, err := c.Publish(c.Topic(string(recipientSubject)), msg); err != nil {
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
		}

		close(in)
	}()
}
