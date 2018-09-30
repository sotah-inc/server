package main

import (
	"fmt"
	"time"

	storage "cloud.google.com/go/storage"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/util"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

func pruneStore(c config, m messenger, s store) error {
	logging.Info("Starting prune-store")

	// establishing a state
	res := newResolver(c, m, s)
	sta := newState(m, res)

	// gathering region-status from the root service
	logging.Info("Gathering regions")
	regions, err := func() (regionList, error) {
		out := regionList{}
		attempts := 0
		for {
			var err error
			out, err = newRegionsFromMessenger(m)
			if err == nil {
				break
			} else {
				logging.Info("Could not fetch regions, retrying in 250ms")

				attempts++
				time.Sleep(250 * time.Millisecond)
			}

			if attempts >= 20 {
				return regionList{}, fmt.Errorf("Failed to fetch regions after %d attempts", attempts)
			}
		}

		return out, nil
	}()
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to fetch regions")

		return err
	}

	sta.regions = c.filterInRegions(regions)

	// filling state with statuses
	for _, reg := range regions {
		regionStatus, err := reg.getStatus(res)
		if err != nil {
			logging.WithField("region", reg.Name).Info("Could not fetch status for region")

			return err
		}
		sta.statuses[reg.Name] = regionStatus
	}

	// establishing channels
	out := make(chan error)
	in := make(chan *storage.BucketHandle)

	// spinning up the workers
	worker := func() {
		for bkt := range in {
			i := 0
			it := bkt.Objects(s.context, nil)
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
				if err := obj.Delete(s.context); err != nil {
					out <- err
				}

				i++
			}

			if err := bkt.Delete(s.context); err != nil {
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
		for _, reg := range sta.regions {
			if reg.Primary {
				continue
			}

			for _, rea := range sta.statuses[reg.Name].Realms {
				exists, err := s.realmAuctionsBucketExists(rea)
				if err != nil {
					out <- err
				}

				if !exists {
					continue
				}

				logging.WithFields(logrus.Fields{"region": reg.Name, "realm": rea.Slug}).Debug("Removing realm-auctions from store")

				bkt := s.getRealmAuctionsBucket(rea)
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
