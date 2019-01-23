package command

import (
	"fmt"
	"time"

	"github.com/sotah-inc/server/app/pkg/state"

	storage "cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
	"google.golang.org/api/iterator"
)

func pruneStore(c internal.Config, m messenger.Messenger, s store.Store) error {
	logging.Info("Starting prune-store")

	// establishing a state
	res := internal.NewResolver(c, m, s)
	sta := state.NewState(res)

	// gathering region-status from the root service
	logging.Info("Gathering regions")
	regions, err := func() (internal.RegionList, error) {
		out := internal.RegionList{}
		attempts := 0
		for {
			var err error
			out, err = internal.NewRegionsFromMessenger(m)
			if err == nil {
				break
			} else {
				logging.Info("Could not fetch regions, retrying in 250ms")

				attempts++
				time.Sleep(250 * time.Millisecond)
			}

			if attempts >= 20 {
				return internal.RegionList{}, fmt.Errorf("Failed to fetch regions after %d attempts", attempts)
			}
		}

		return out, nil
	}()
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to fetch regions")

		return err
	}

	sta.Regions = c.FilterInRegions(regions)

	// filling state with statuses
	for _, reg := range regions {
		regionStatus, err := reg.GetStatus(res)
		if err != nil {
			logging.WithField("region", reg.Name).Info("Could not fetch status for region")

			return err
		}
		sta.Statuses[reg.Name] = regionStatus
	}

	// establishing channels
	out := make(chan error)
	in := make(chan *storage.BucketHandle)

	// spinning up the workers
	worker := func() {
		for bkt := range in {
			i := 0
			it := bkt.Objects(s.Context, nil)
			for {
				if i == 0 || i%5 == 0 {
					logging.WithField("count", i).Debug("Deleted object from bucket")
				}

				objAttrs, err := it.Next()
				if err != nil {
					if err == iterator.Done {
						break
					}

					logging.WithField("error", err.Error()).Error("Failed to iterate over item objects")

					continue
				}

				obj := bkt.Object(objAttrs.Name)
				if err := obj.Delete(s.Context); err != nil {
					out <- err
				}

				i++
			}

			if err := bkt.Delete(s.Context); err != nil {
				out <- err
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing up
	go func() {
		// going over statuses to prune buckets
		for _, reg := range sta.Regions {
			if reg.Primary {
				continue
			}

			for _, rea := range sta.Statuses[reg.Name].Realms {
				exists, err := s.RealmAuctionsBucketExists(rea)
				if err != nil {
					out <- err
				}

				if !exists {
					continue
				}

				logging.WithFields(logrus.Fields{"region": reg.Name, "realm": rea.Slug}).Debug("Removing realm-auctions from store")

				bkt := s.GetRealmAuctionsBucket(rea)
				in <- bkt
			}
		}

		close(in)
	}()

	// going over results
	for err := range out {
		logging.WithField("error", err.Error()).Error("Failed")
	}

	return nil
}
