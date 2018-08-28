package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/boltdb/bolt"
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

func newState(mess messenger, res resolver) state {
	return state{
		messenger:   mess,
		resolver:    res,
		regions:     res.config.Regions,
		statuses:    statuses{},
		auctions:    map[regionName]map[blizzard.RealmSlug]miniAuctionList{},
		items:       map[blizzard.ItemID]item{},
		expansions:  res.config.Expansions,
		professions: res.config.Professions,
	}
}

type state struct {
	messenger messenger
	resolver  resolver
	listeners listeners
	databases databases

	regions     []region
	statuses    statuses
	auctions    map[regionName]map[blizzard.RealmSlug]miniAuctionList
	items       itemsMap
	itemClasses blizzard.ItemClasses
	expansions  []expansion
	professions []profession
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

type bootResponse struct {
	Regions     regionList           `json:"regions"`
	ItemClasses blizzard.ItemClasses `json:"item_classes"`
	Expansions  []expansion          `json:"expansions"`
	Professions []profession         `json:"professions"`
}

func (sta state) listenForBoot(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.Boot, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		encodedResponse, err := json.Marshal(bootResponse{
			Regions:     sta.regions,
			ItemClasses: sta.itemClasses,
			Expansions:  sta.expansions,
			Professions: sta.professions,
		})
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedResponse)
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

type auctionsIntakeResult struct {
	itemIds              []blizzard.ItemID
	removedAuctionsCount int
}

func (sta state) auctionsIntake(job getAuctionsJob) (auctionsIntakeResult, error) {
	rea := job.realm
	reg := rea.region

	// storing deleted auction ids for calculating the churn rate
	removedAuctionIds := map[int64]struct{}{}
	for _, mAuction := range sta.auctions[reg.Name][rea.Slug] {
		for _, auc := range mAuction.AucList {
			removedAuctionIds[auc] = struct{}{}
		}
	}
	for _, auc := range job.auctions.Auctions {
		if _, ok := removedAuctionIds[auc.Auc]; ok {
			delete(removedAuctionIds, auc.Auc)
		}
	}

	// compacting the auctions
	minimizedAuctions := newMiniAuctionListFromBlizzardAuctions(job.auctions.Auctions)

	// loading the minimized auctions into state
	sta.auctions[reg.Name][rea.Slug] = minimizedAuctions

	// setting the realm last-modified
	for i, statusRealm := range sta.statuses[reg.Name].Realms {
		if statusRealm.Slug != rea.Slug {
			continue
		}

		sta.statuses[reg.Name].Realms[i].LastModified = job.lastModified.Unix()

		break
	}

	// misc
	itemIds := minimizedAuctions.itemIds()

	// writing pricelists to db
	lastModifiedKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(lastModifiedKey, uint64(job.lastModified.Unix()))
	pLists := newPriceList(itemIds, minimizedAuctions)
	db := sta.databases[reg.Name][rea.Slug].db
	log.WithFields(log.Fields{
		"region": reg.Name,
		"realm":  rea.Slug,
		"count":  len(pLists),
	}).Info("Writing pricelists")
	err := db.Batch(func(tx *bolt.Tx) error {
		for itemID, pList := range pLists {
			b, err := tx.CreateBucketIfNotExists([]byte(fmt.Sprintf("item-prices/%d", itemID)))
			if err != nil {
				return err
			}

			result, err := json.Marshal(pList)
			if err != nil {
				return err
			}

			if err = b.Put(lastModifiedKey, result); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return auctionsIntakeResult{}, err
	}

	// returning a list of item ids for syncing
	return auctionsIntakeResult{itemIds: itemIds}, nil
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
	log.Info("Stopping listeners")

	for _, l := range ls {
		l.stopChan <- struct{}{}
	}
}

type listener struct {
	call     listenFunc
	stopChan listenStopChan
}

type workerStopChan chan interface{}
