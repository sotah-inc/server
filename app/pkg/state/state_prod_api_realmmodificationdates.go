package state

import (
	"encoding/json"
	"errors"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func NewRealmModificationDatesRequest(data []byte) (RealmModificationDatesRequest, error) {
	var r RealmModificationDatesRequest
	if err := json.Unmarshal(data, &r); err != nil {
		return RealmModificationDatesRequest{}, err
	}

	return r, nil
}

type RealmModificationDatesRequest struct {
	RegionName string `json:"region_name"`
	RealmSlug  string `json:"realm_slug"`
}

type RealmModificationDatesResponse struct {
	sotah.RealmModificationDates
}

func (r RealmModificationDatesResponse) EncodeForDelivery() ([]byte, error) {
	return json.Marshal(r)
}

func (sta ProdApiState) ListenForRealmModificationDates(stop ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(string(subjects.RealmModificationDates), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		req, err := NewRealmModificationDatesRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.GenericError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		regionName, err := func() (blizzard.RegionName, error) {
			for _, region := range sta.Regions {
				if region.Name != blizzard.RegionName(req.RegionName) {
					continue
				}

				if _, ok := sta.Statuses[blizzard.RegionName(req.RegionName)]; !ok {
					continue
				}

				return region.Name, nil
			}

			return blizzard.RegionName(""), errors.New("region not found")
		}()
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.UserError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		realm, err := func() (sotah.Realm, error) {
			for _, realm := range sta.Statuses[regionName].Realms {
				if realm.Slug != blizzard.RealmSlug(req.RealmSlug) {
					continue
				}

				return realm, nil
			}

			return sotah.Realm{}, errors.New("realm not found")
		}()
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.UserError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		res := RealmModificationDatesResponse{RealmModificationDates: realm.RealmModificationDates}

		encodedData, err := res.EncodeForDelivery()
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.GenericError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedData)
		sta.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
