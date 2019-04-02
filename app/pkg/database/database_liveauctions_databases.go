package database

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/database/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/sotah/sortdirections"
	"github.com/sotah-inc/server/app/pkg/sotah/sortkinds"
	"github.com/sotah-inc/server/app/pkg/util"
)

func NewLiveAuctionsDatabases(dirPath string, stas sotah.Statuses) (LiveAuctionsDatabases, error) {
	ladBases := LiveAuctionsDatabases{}

	for regionName, status := range stas {
		ladBases[regionName] = map[blizzard.RealmSlug]liveAuctionsDatabase{}

		for _, rea := range status.Realms {
			ladBase, err := newLiveAuctionsDatabase(dirPath, rea)
			if err != nil {
				return LiveAuctionsDatabases{}, err
			}

			ladBases[regionName][rea.Slug] = ladBase
		}
	}

	return ladBases, nil
}

type LiveAuctionsDatabases map[blizzard.RegionName]map[blizzard.RealmSlug]liveAuctionsDatabase

type liveAuctionsLoadOutJob struct {
	Err                  error
	Realm                sotah.Realm
	LastModified         time.Time
	Stats                miniAuctionListStats
	TotalRemovedAuctions int
	TotalNewAuctions     int
}

func (job liveAuctionsLoadOutJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":         job.Err.Error(),
		"region":        job.Realm.Region.Name,
		"realm":         job.Realm.Slug,
		"last-modified": job.LastModified.Unix(),
	}
}

func (ladBases LiveAuctionsDatabases) Load(in chan LoadInJob) chan liveAuctionsLoadOutJob {
	// establishing channels
	out := make(chan liveAuctionsLoadOutJob)

	// spinning up workers for receiving auctions and persisting them
	worker := func() {
		for job := range in {
			// resolving the live-auctions database and gathering current Stats
			ladBase := ladBases[job.Realm.Region.Name][job.Realm.Slug]
			malStats, err := ladBase.stats()
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.Realm.Region.Name,
					"realm":  job.Realm.Slug,
				}).Error("Failed to gather live-auctions stats")

				out <- liveAuctionsLoadOutJob{
					Err:                  err,
					Realm:                job.Realm,
					LastModified:         job.TargetTime,
					Stats:                miniAuctionListStats{},
					TotalRemovedAuctions: 0,
					TotalNewAuctions:     0,
				}

				continue
			}

			// gathering previous and new auction counts for metrics collection
			totalNewAuctions, totalRemovedAuctions := func() (int, int) {
				removedAuctionIds := map[int64]struct{}{}
				for _, auc := range malStats.AuctionIds {
					removedAuctionIds[auc] = struct{}{}
				}
				newAuctionIds := map[int64]struct{}{}
				for _, auc := range job.Auctions.Auctions {
					if _, ok := removedAuctionIds[auc.Auc]; ok {
						delete(removedAuctionIds, auc.Auc)
					}

					newAuctionIds[auc.Auc] = struct{}{}
				}
				for _, auc := range malStats.AuctionIds {
					if _, ok := newAuctionIds[auc]; ok {
						delete(newAuctionIds, auc)
					}
				}

				return len(newAuctionIds), len(removedAuctionIds)
			}()

			maList := sotah.NewMiniAuctionListFromMiniAuctions(sotah.NewMiniAuctions(job.Auctions))
			if err := ladBase.persistMiniAuctionList(maList); err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.Realm.Region.Name,
					"realm":  job.Realm.Slug,
				}).Error("Failed to persist mini-auction-list")

				out <- liveAuctionsLoadOutJob{
					Err:                  err,
					Realm:                job.Realm,
					LastModified:         job.TargetTime,
					Stats:                miniAuctionListStats{},
					TotalRemovedAuctions: 0,
					TotalNewAuctions:     0,
				}

				continue
			}

			out <- liveAuctionsLoadOutJob{
				Err:                  nil,
				Realm:                job.Realm,
				LastModified:         job.TargetTime,
				TotalNewAuctions:     totalNewAuctions,
				TotalRemovedAuctions: totalRemovedAuctions,
				Stats:                malStats,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(2, worker, postWork)

	return out
}

type LiveAuctionsLoadEncodedDataInJob struct {
	RegionName  blizzard.RegionName
	RealmSlug   blizzard.RealmSlug
	EncodedData []byte
}

type LiveAuctionsLoadEncodedDataOutJob struct {
	Err        error
	RegionName blizzard.RegionName
	RealmSlug  blizzard.RealmSlug
}

func (job LiveAuctionsLoadEncodedDataOutJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":  job.Err.Error(),
		"region": job.RegionName,
		"realm":  job.RealmSlug,
	}
}

func (ladBases LiveAuctionsDatabases) LoadEncodedData(in chan LiveAuctionsLoadEncodedDataInJob) chan LiveAuctionsLoadEncodedDataOutJob {
	// establishing channels
	out := make(chan LiveAuctionsLoadEncodedDataOutJob)

	// spinning up workers for receiving encoded-data and persisting it
	worker := func() {
		for job := range in {
			// resolving the live-auctions database and gathering current Stats
			ladBase := ladBases[job.RegionName][job.RealmSlug]

			if err := ladBase.persistEncodedData(job.EncodedData); err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.RegionName,
					"realm":  job.RealmSlug,
				}).Error("Failed to persist encoded-data")

				out <- LiveAuctionsLoadEncodedDataOutJob{
					Err:        err,
					RegionName: job.RegionName,
					RealmSlug:  job.RealmSlug,
				}

				continue
			}

			out <- LiveAuctionsLoadEncodedDataOutJob{
				Err:        nil,
				RegionName: job.RegionName,
				RealmSlug:  job.RealmSlug,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	return out
}

type GetStatsJob struct {
	Err   error
	Realm sotah.Realm
	Stats miniAuctionListStats
}

func (job GetStatsJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":  job.Err.Error(),
		"region": job.Realm.Region.Name,
		"realm":  job.Realm.Slug,
	}
}

func (ladBases LiveAuctionsDatabases) GetStats(realms sotah.Realms) chan GetStatsJob {
	in := make(chan sotah.Realm)
	out := make(chan GetStatsJob)

	worker := func() {
		for rea := range in {
			stats, err := ladBases[rea.Region.Name][rea.Slug].stats()
			out <- GetStatsJob{err, rea, stats}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	go func() {
		for _, rea := range realms {
			in <- rea
		}

		close(in)
	}()

	return out
}

func NewQueryRequest(data []byte) (QueryAuctionsRequest, error) {
	ar := &QueryAuctionsRequest{}
	err := json.Unmarshal(data, &ar)
	if err != nil {
		return QueryAuctionsRequest{}, err
	}

	return *ar, nil
}

type QueryAuctionsRequest struct {
	RegionName    blizzard.RegionName          `json:"region_name"`
	RealmSlug     blizzard.RealmSlug           `json:"realm_slug"`
	Page          int                          `json:"page"`
	Count         int                          `json:"count"`
	SortDirection sortdirections.SortDirection `json:"sort_direction"`
	SortKind      sortkinds.SortKind           `json:"sort_kind"`
	OwnerFilters  []sotah.OwnerName            `json:"owner_filters"`
	ItemFilters   []blizzard.ItemID            `json:"item_filters"`
}

type QueryAuctionsResponse struct {
	AuctionList sotah.MiniAuctionList `json:"auctions"`
	Total       int                   `json:"total"`
	TotalCount  int                   `json:"total_count"`
}

func (qr QueryAuctionsResponse) EncodeForDelivery() (string, error) {
	jsonEncoded, err := json.Marshal(qr)
	if err != nil {
		return "", err
	}

	gzipEncoded, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(gzipEncoded), nil
}

func (ladBases LiveAuctionsDatabases) QueryAuctions(qr QueryAuctionsRequest) (QueryAuctionsResponse, codes.Code, error) {
	regionLadBases, ok := ladBases[qr.RegionName]
	if !ok {
		return QueryAuctionsResponse{}, codes.UserError, errors.New("invalid region")
	}

	realmLadbase, ok := regionLadBases[qr.RealmSlug]
	if !ok {
		return QueryAuctionsResponse{}, codes.UserError, errors.New("invalid realm")
	}

	if qr.Page < 0 {
		return QueryAuctionsResponse{}, codes.UserError, errors.New("page must be >= 0")
	}
	if qr.Count == 0 {
		return QueryAuctionsResponse{}, codes.UserError, errors.New("count must be >= 0")
	} else if qr.Count > 1000 {
		return QueryAuctionsResponse{}, codes.UserError, errors.New("page must be <= 1000")
	}

	maList, err := realmLadbase.GetMiniAuctionList()
	if err != nil {
		return QueryAuctionsResponse{}, codes.GenericError, err
	}

	// initial response format
	aResponse := QueryAuctionsResponse{Total: -1, TotalCount: -1, AuctionList: maList}

	// filtering in auctions by owners or items
	if len(qr.OwnerFilters) > 0 {
		aResponse.AuctionList = aResponse.AuctionList.FilterByOwnerNames(qr.OwnerFilters)
	}
	if len(qr.ItemFilters) > 0 {
		aResponse.AuctionList = aResponse.AuctionList.FilterByItemIDs(qr.ItemFilters)
	}

	// calculating the total for paging
	aResponse.Total = len(aResponse.AuctionList)

	// calculating the total-count for review
	totalCount := 0
	for _, mAuction := range maList {
		totalCount += len(mAuction.AucList)
	}
	aResponse.TotalCount = totalCount

	// optionally sorting
	if qr.SortKind != sortkinds.None && qr.SortDirection != sortdirections.None {
		err = aResponse.AuctionList.Sort(qr.SortKind, qr.SortDirection)
		if err != nil {
			return QueryAuctionsResponse{}, codes.UserError, err
		}
	}

	// truncating the list
	aResponse.AuctionList, err = aResponse.AuctionList.Limit(qr.Count, qr.Page)
	if err != nil {
		return QueryAuctionsResponse{}, codes.UserError, err
	}

	return aResponse, codes.Ok, nil
}

func NewGetPricelistRequest(data []byte) (GetPricelistRequest, error) {
	plRequest := &GetPricelistRequest{}
	err := json.Unmarshal(data, &plRequest)
	if err != nil {
		return GetPricelistRequest{}, err
	}

	return *plRequest, nil
}

type GetPricelistRequest struct {
	RegionName blizzard.RegionName `json:"region_name"`
	RealmSlug  blizzard.RealmSlug  `json:"realm_slug"`
	ItemIds    []blizzard.ItemID   `json:"item_ids"`
}

type GetPricelistResponse struct {
	Pricelist sotah.ItemPrices `json:"price_list"`
}

func (plResponse GetPricelistResponse) EncodeForDelivery() (string, error) {
	jsonEncoded, err := json.Marshal(plResponse)
	if err != nil {
		return "", err
	}

	gzipEncoded, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(gzipEncoded), nil
}

func (ladBases LiveAuctionsDatabases) GetPricelist(plRequest GetPricelistRequest) (GetPricelistResponse, codes.Code, error) {
	regionLadBases, ok := ladBases[plRequest.RegionName]
	if !ok {
		return GetPricelistResponse{}, codes.UserError, errors.New("invalid region")
	}

	ladBase, ok := regionLadBases[plRequest.RealmSlug]
	if !ok {
		return GetPricelistResponse{}, codes.UserError, errors.New("invalid realm")
	}

	maList, err := ladBase.GetMiniAuctionList()
	if err != nil {
		return GetPricelistResponse{}, codes.GenericError, err
	}

	iPrices := sotah.NewItemPrices(maList)
	responseItemPrices := sotah.ItemPrices{}
	for _, itemId := range plRequest.ItemIds {
		if iPrice, ok := iPrices[itemId]; ok {
			responseItemPrices[itemId] = iPrice

			continue
		}
	}

	return GetPricelistResponse{Pricelist: responseItemPrices}, codes.Ok, nil
}

func NewQueryOwnersByItemsRequest(data []byte) (QueryOwnersByItemsRequest, error) {
	req := &QueryOwnersByItemsRequest{}
	err := json.Unmarshal(data, &req)
	if err != nil {
		return QueryOwnersByItemsRequest{}, err
	}

	return *req, nil
}

type QueryOwnersByItemsRequest struct {
	RegionName blizzard.RegionName `json:"region_name"`
	RealmSlug  blizzard.RealmSlug  `json:"realm_slug"`
	Items      []blizzard.ItemID   `json:"items"`
}

type ownerItemsOwnership struct {
	OwnedValue  int64 `json:"owned_value"`
	OwnedVolume int64 `json:"owned_volume"`
}

type QueryOwnersByItemsResponse struct {
	Ownership   map[sotah.OwnerName]ownerItemsOwnership `json:"ownership"`
	TotalValue  int64                                   `json:"total_value"`
	TotalVolume int64                                   `json:"total_volume"`
}

func (plResponse QueryOwnersByItemsResponse) EncodeForDelivery() (string, error) {
	jsonEncoded, err := json.Marshal(plResponse)
	if err != nil {
		return "", err
	}

	return string(jsonEncoded), nil
}

func (ladBases LiveAuctionsDatabases) QueryOwnersByItems(req QueryOwnersByItemsRequest) (QueryOwnersByItemsResponse, codes.Code, error) {
	regionLadBases, ok := ladBases[req.RegionName]
	if !ok {
		return QueryOwnersByItemsResponse{}, codes.UserError, errors.New("invalid region")
	}

	ladBase, ok := regionLadBases[req.RealmSlug]
	if !ok {
		return QueryOwnersByItemsResponse{}, codes.UserError, errors.New("invalid realm")
	}

	maList, err := ladBase.GetMiniAuctionList()
	if err != nil {
		return QueryOwnersByItemsResponse{}, codes.GenericError, err
	}

	iMap := sotah.ItemIdsMap{}
	result := QueryOwnersByItemsResponse{
		Ownership:   map[sotah.OwnerName]ownerItemsOwnership{},
		TotalValue:  0,
		TotalVolume: 0,
	}
	for _, mAuction := range maList {
		if _, ok := iMap[mAuction.ItemID]; !ok {
			continue
		}

		aucListValue := mAuction.Buyout * mAuction.Quantity * int64(len(mAuction.AucList))
		aucListVolume := int64(len(mAuction.AucList)) * mAuction.Quantity

		result.TotalValue += aucListValue
		result.TotalVolume += aucListVolume

		if _, ok := result.Ownership[mAuction.Owner]; !ok {
			result.Ownership[mAuction.Owner] = ownerItemsOwnership{0, 0}
		}

		result.Ownership[mAuction.Owner] = ownerItemsOwnership{
			OwnedValue:  result.Ownership[mAuction.Owner].OwnedValue + aucListValue,
			OwnedVolume: result.Ownership[mAuction.Owner].OwnedVolume + aucListVolume,
		}
	}

	return result, codes.Ok, nil
}
