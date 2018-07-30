package main

import (
	"encoding/json"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"
)

type requestError struct {
	code    codes.Code
	message string
}

type state struct {
	messenger messenger
	resolver  resolver
	listeners listeners

	regions     []region
	statuses    map[regionName]status
	auctions    map[regionName]map[blizzard.RealmSlug]miniAuctionList
	items       itemsMap
	itemClasses blizzard.ItemClasses
}

func (sta state) listenForRegions(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.Regions, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		encodedRegions, err := json.Marshal(sta.regions)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedRegions)
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta state) listenForGenericTestErrors(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.GenericTestErrors, stop, func(natsMsg nats.Msg) {
		m := newMessage()
		m.Err = "Test error"
		m.Code = codes.GenericError
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta state) auctionsIntake(job getAuctionsJob) []blizzard.ItemID {
	rea := job.realm
	reg := rea.region
	if job.err != nil {
		log.WithFields(log.Fields{
			"region": reg.Name,
			"realm":  rea.Slug,
			"error":  job.err.Error(),
		}).Info("Auction fetch failure")

		return []blizzard.ItemID{}
	}

	// compacting the auctions
	minimizedAuctions := newMiniAuctionListFromBlizzardAuctions(job.auctions.Auctions)

	// loading the minimized auctions into state
	sta.auctions[reg.Name][rea.Slug] = minimizedAuctions

	// returning a list of item ids for syncing
	return minimizedAuctions.itemIds()
}

func (sta state) collectRegions(res resolver) {
	// going over the list of regions
	for _, reg := range sta.regions {
		// misc
		regionItemIDsMap := map[blizzard.ItemID]struct{}{}

		// downloading auctions in a region
		wList := res.config.getRegionWhitelist(reg)
		log.WithFields(log.Fields{
			"region":    reg.Name,
			"realms":    len(sta.statuses[reg.Name].Realms),
			"whitelist": wList,
		}).Info("Downloading region")
		auctionsOut := sta.statuses[reg.Name].Realms.getAuctionsOrAll(sta.resolver, wList)
		for job := range auctionsOut {
			itemIDs := sta.auctionsIntake(job)
			for _, ID := range itemIDs {
				_, ok := sta.items[ID]
				if ok {
					continue
				}

				regionItemIDsMap[ID] = struct{}{}
			}
		}
		log.WithField("region", reg.Name).Info("Downloaded region")

		// gathering the list of item IDs for this region
		regionItemIDs := make([]blizzard.ItemID, len(regionItemIDsMap))
		i := 0
		for ID := range regionItemIDsMap {
			regionItemIDs[i] = ID
			i++
		}

		// downloading items found in this region
		log.WithField("items", len(regionItemIDs)).Info("Fetching items")
		itemsOut := getItems(regionItemIDs, res)
		for job := range itemsOut {
			if job.err != nil {
				log.WithFields(log.Fields{
					"region": reg.Name,
					"item":   job.ID,
					"error":  job.err.Error(),
				}).Info("Failed to fetch item")

				continue
			}

			sta.items[job.ID] = job.item
		}
		log.WithField("items", len(regionItemIDs)).Info("Fetched items")

		// downloading item icons found in this region
		iconNames := sta.items.getItemIcons()
		log.WithField("items", len(iconNames)).Info("Syncing item icons")
		itemIconsOut := syncItemIcons(iconNames, res)
		for job := range itemIconsOut {
			if job.err != nil {
				log.WithFields(log.Fields{
					"item":  job.icon,
					"error": job.err.Error(),
				}).Info("Failed to sync item icon")

				continue
			}
		}
		log.WithField("items", len(iconNames)).Info("Synced item icons")
	}
}

type listenStopChan chan interface{}

type listenFunc func(stop listenStopChan) error

type subjectListeners map[subjects.Subject]listenFunc

func newListeners(sListeners subjectListeners) listeners {
	ls := listeners{}
	for subj, l := range sListeners {
		ls[subj] = listener{l, make(listenStopChan)}
	}

	return ls
}

type listeners map[subjects.Subject]listener

func (ls listeners) listen() error {
	log.WithField("listeners", len(ls)).Info("Starting listeners")

	for _, l := range ls {
		if err := l.call(l.stopChan); err != nil {
			return err
		}
	}

	return nil
}

func (ls listeners) stop() {
	for _, l := range ls {
		l.stopChan <- struct{}{}
	}
}

type listener struct {
	call     listenFunc
	stopChan listenStopChan
}
