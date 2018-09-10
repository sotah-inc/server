package main

import (
	"fmt"
	"time"

	"github.com/ihsw/sotah-server/app/logging"
	"github.com/sirupsen/logrus"
)

func pruneStore(c config, m messenger, s store) error {
	logging.Info("Starting prune-store")

	// establishing a state
	res := newResolver(c, m, s)
	sta := newState(m, res)

	// gathering region-status from the root service
	regions := []*region{}
	attempts := 0
	for {
		var err error
		regions, err = newRegionsFromMessenger(m)
		if err == nil {
			break
		} else {
			logging.Info("Could not fetch regions, retrying in 250ms")

			attempts++
			time.Sleep(250 * time.Millisecond)
		}

		if attempts >= 20 {
			return fmt.Errorf("Failed to fetch regions after %d attempts", attempts)
		}
	}

	for i, reg := range regions {
		sta.regions[i] = *reg
	}

	// filling state with statuses
	for _, reg := range regions {
		regionStatus, err := reg.getStatus(res)
		if err != nil {
			logging.WithField("region", reg.Name).Info("Could not fetch status for region")

			return err
		}
		sta.statuses[reg.Name] = regionStatus
	}

	// going over statuses to prune buckets
	for _, reg := range sta.regions {
		if reg.Primary {
			continue
		}

		for _, rea := range sta.statuses[reg.Name].Realms {
			exists, err := s.realmAuctionsBucketExists(rea)
			if err != nil {
				return err
			}

			if !exists {
				continue
			}

			logging.WithFields(logrus.Fields{"region": reg.Name, "realm": rea.Slug}).Debug("Removing realm-auctions from store")

			bkt := s.getRealmAuctionsBucket(rea)
			if err := bkt.Delete(s.context); err != nil {
				return err
			}
		}
	}

	return nil
}
