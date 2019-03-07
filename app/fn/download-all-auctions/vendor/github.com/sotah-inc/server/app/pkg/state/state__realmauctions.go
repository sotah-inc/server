package state

import (
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
)

type GetAuctionsFromTimesOutJob struct {
	Err        error
	Realm      sotah.Realm
	TargetTime time.Time
	Auctions   blizzard.Auctions
}

func (job GetAuctionsFromTimesOutJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":       job.Err.Error(),
		"realm":       job.Realm.Slug,
		"target_time": job.TargetTime.Unix(),
	}
}

func (sta State) GetAuctionsFromTimes(times RegionRealmTimes) chan GetAuctionsFromTimesOutJob {
	in := make(chan RealmTimeTuple)
	out := make(chan GetAuctionsFromTimesOutJob)

	// spinning up the workers for fetching Auctions
	worker := func() {
		for timeTuple := range in {
			if !sta.UseGCloud {
				aucs, lastModified, err := sta.IO.DiskStore.GetAuctionsByRealm(timeTuple.Realm)
				if err != nil {
					out <- GetAuctionsFromTimesOutJob{
						Err:        err,
						Realm:      timeTuple.Realm,
						TargetTime: time.Unix(0, 0),
						Auctions:   blizzard.Auctions{},
					}

					continue
				}

				out <- GetAuctionsFromTimesOutJob{
					Realm:      timeTuple.Realm,
					TargetTime: lastModified,
					Auctions:   aucs,
				}

				continue
			}

			aucs, err := sta.IO.StoreClient.GetAuctions(timeTuple.Realm, timeTuple.TargetTime)
			if err != nil {
				out <- GetAuctionsFromTimesOutJob{
					Err:        err,
					Realm:      timeTuple.Realm,
					TargetTime: timeTuple.TargetTime,
					Auctions:   blizzard.Auctions{},
				}

				continue
			}

			out <- GetAuctionsFromTimesOutJob{
				Err:        nil,
				Realm:      timeTuple.Realm,
				TargetTime: timeTuple.TargetTime,
				Auctions:   aucs,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the Realms
	go func() {
		for regionName, realmTimes := range times {
			for realmSlug, timeTuple := range realmTimes {
				logging.WithFields(logrus.Fields{
					"region": regionName,
					"realm":  realmSlug,
				}).Debug("Queueing up auctions for loading")
				in <- timeTuple
			}
		}

		close(in)
	}()

	return out
}

type StoreAuctionsInJob struct {
	Realm      sotah.Realm
	TargetTime time.Time
	Auctions   blizzard.Auctions
}

type StoreAuctionsOutJob struct {
	Err        error
	Realm      sotah.Realm
	TargetTime time.Time
	ItemIds    []blizzard.ItemID
}

func (job StoreAuctionsOutJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":       job.Err.Error(),
		"region":      job.Realm.Region.Name,
		"realm":       job.Realm.Slug,
		"target-time": job.TargetTime.Unix(),
	}
}

func (sta State) StoreAuctions(in chan StoreAuctionsInJob) chan StoreAuctionsOutJob {
	out := make(chan StoreAuctionsOutJob)

	// spinning up the workers for fetching Auctions
	worker := func() {
		for inJob := range in {
			jsonEncodedData, err := json.Marshal(inJob.Auctions)
			if err != nil {
				out <- StoreAuctionsOutJob{
					Err:        err,
					Realm:      inJob.Realm,
					TargetTime: inJob.TargetTime,
					ItemIds:    []blizzard.ItemID{},
				}

				continue
			}

			gzipEncodedData, err := util.GzipEncode(jsonEncodedData)
			if err != nil {
				out <- StoreAuctionsOutJob{
					Err:        err,
					Realm:      inJob.Realm,
					TargetTime: inJob.TargetTime,
					ItemIds:    []blizzard.ItemID{},
				}

				continue
			}

			err = func() error {
				if sta.UseGCloud {
					return sta.IO.StoreClient.WriteRealmAuctions(inJob.Realm, inJob.TargetTime, gzipEncodedData)
				}

				return sta.IO.DiskStore.WriteAuctions(inJob.Realm, gzipEncodedData)
			}()

			if err != nil {
				out <- StoreAuctionsOutJob{
					Err:        err,
					Realm:      inJob.Realm,
					TargetTime: inJob.TargetTime,
					ItemIds:    []blizzard.ItemID{},
				}

				continue
			}

			outItemIds := []blizzard.ItemID{}
			for _, auc := range inJob.Auctions.Auctions {
				outItemIds = append(outItemIds, auc.Item)
			}

			out <- StoreAuctionsOutJob{
				Err:        nil,
				Realm:      inJob.Realm,
				TargetTime: inJob.TargetTime,
				ItemIds:    outItemIds,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	return out
}
