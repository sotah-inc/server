package state

import (
	"encoding/json"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/blizzard"
	"github.com/sotah-inc/server/app/codes"
	"github.com/sotah-inc/server/app/subjects"
)

func (sta State) listenForRegions(stop ListenStopChan) error {
	err := sta.Messenger.subscribe(subjects.Regions, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		encodedRegions, err := json.Marshal(sta.Regions)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedRegions)
		sta.Messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

type bootResponse struct {
	Regions     regionList           `json:"Regions"`
	ItemClasses blizzard.ItemClasses `json:"item_classes"`
	Expansions  []expansion          `json:"expansions"`
	Professions []profession         `json:"professions"`
}

func (sta State) listenForBoot(stop ListenStopChan) error {
	err := sta.Messenger.subscribe(subjects.Boot, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		encodedResponse, err := json.Marshal(bootResponse{
			Regions:     sta.Regions,
			ItemClasses: sta.itemClasses,
			Expansions:  sta.expansions,
			Professions: sta.professions,
		})
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedResponse)
		sta.Messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta State) listenForGenericTestErrors(stop ListenStopChan) error {
	err := sta.Messenger.subscribe(subjects.GenericTestErrors, stop, func(natsMsg nats.Msg) {
		m := newMessage()
		m.Err = "Test error"
		m.Code = codes.GenericError
		sta.Messenger.replyTo(natsMsg, m)
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

func (sta State) auctionsIntake(job getAuctionsJob) (auctionsIntakeResult, error) {
	rea := job.realm
	reg := rea.region

	// setting the Realm last-modified
	for i, statusRealm := range sta.Statuses[reg.Name].Realms {
		if statusRealm.Slug != rea.Slug {
			continue
		}

		sta.Statuses[reg.Name].Realms[i].LastModified = job.lastModified.Unix()

		break
	}

	// gathering item-ids for item fetching
	itemIdsMap := map[blizzard.ItemID]struct{}{}
	for _, auc := range job.auctions.Auctions {
		itemIdsMap[auc.Item] = struct{}{}
	}
	itemIds := make([]blizzard.ItemID, len(itemIdsMap))
	i := 0
	for ID := range itemIdsMap {
		itemIds[i] = ID
		i++
	}

	// returning a list of item ids for syncing
	return auctionsIntakeResult{itemIds: itemIds}, nil
}
