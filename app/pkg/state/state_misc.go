package state

import (
	"encoding/json"
	"errors"
	"github.com/sotah-inc/server/app/pkg/sotah"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
)

func (sta State) ListenForRegions(stop ListenStopChan) error {
	err := sta.Messenger.Subscribe(subjects.Regions, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		encodedRegions, err := json.Marshal(sta.Regions)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedRegions)
		sta.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta State) NewRegions() (sotah.RegionList, error) {
	msg, err := sta.messenger.Request(subjects.Regions, []byte{})
	if err != nil {
		return sotah.RegionList{}, err
	}

	if msg.Code != codes.Ok {
		return nil, errors.New(msg.Err)
	}

	regs := sotah.RegionList{}
	if err := json.Unmarshal([]byte(msg.Data), &regs); err != nil {
		return sotah.RegionList{}, err
	}

	return regs, nil
}

type bootResponse struct {
	Regions     internal.RegionList   `json:"Regions"`
	ItemClasses blizzard.ItemClasses  `json:"item_classes"`
	Expansions  []internal.Expansion  `json:"expansions"`
	Professions []internal.Profession `json:"professions"`
}

func (sta State) ListenForBoot(stop ListenStopChan) error {
	err := sta.Messenger.Subscribe(subjects.Boot, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		encodedResponse, err := json.Marshal(bootResponse{
			Regions:     sta.Regions,
			ItemClasses: sta.ItemClasses,
			Expansions:  sta.expansions,
			Professions: sta.professions,
		})
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedResponse)
		sta.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta State) ListenForGenericTestErrors(stop ListenStopChan) error {
	err := sta.Messenger.Subscribe(subjects.GenericTestErrors, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()
		m.Err = "Test error"
		m.Code = codes.GenericError
		sta.Messenger.ReplyTo(natsMsg, m)
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

func (sta State) auctionsIntake(job internal.GetAuctionsJob) (auctionsIntakeResult, error) {
	rea := job.Realm
	reg := rea.Region

	// setting the Realm last-modified
	for i, statusRealm := range sta.Statuses[reg.Name].Realms {
		if statusRealm.Slug != rea.Slug {
			continue
		}

		sta.Statuses[reg.Name].Realms[i].LastModified = job.LastModified.Unix()

		break
	}

	// gathering item-ids for item fetching
	itemIdsMap := map[blizzard.ItemID]struct{}{}
	for _, auc := range job.Auctions.Auctions {
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
