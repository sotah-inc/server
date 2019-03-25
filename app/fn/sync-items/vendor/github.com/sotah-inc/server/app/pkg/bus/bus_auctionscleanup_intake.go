package bus

import (
	"encoding/json"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/util"
)

type LoadAuctionsCleanupJobsOutJob struct {
	Err error
	Job CleanupAuctionManifestJob
}

func (c Client) LoadAuctionsCleanupJobs(jobs []CleanupAuctionManifestJob, recipientTopic *pubsub.Topic) chan LoadAuctionsCleanupJobsOutJob {
	// establishing channels
	in := make(chan CleanupAuctionManifestJob)
	out := make(chan LoadAuctionsCleanupJobsOutJob)

	// spinning up the workers
	worker := func() {
		for inJob := range in {
			jsonEncoded, err := json.Marshal(inJob)
			if err != nil {
				logging.WithField("error", err.Error()).Fatal("Failed to encode job")

				return
			}

			msg := NewMessage()
			msg.Data = string(jsonEncoded)

			logging.WithFields(logrus.Fields{
				"region":   inJob.RegionName,
				"realm":    inJob.RealmSlug,
				"manifest": inJob.TargetTimestamp,
			}).Info("Queueing up job")
			if _, err := c.Publish(recipientTopic, msg); err != nil {
				logging.WithField("error", err.Error()).Fatal("Failed to publish message")

				return
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(16, worker, postWork)

	// queueing up the jobs
	go func() {
		logging.Info("Queueing up jobs")
		for _, job := range jobs {
			in <- job
		}

		close(in)
	}()

	return out
}
