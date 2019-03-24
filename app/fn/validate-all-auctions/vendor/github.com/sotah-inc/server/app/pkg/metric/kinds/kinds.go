package kinds

type Kind string

const (
	Collector                       Kind = "collector"
	LiveAuctionsIntake              Kind = "liveauctions_intake"
	PricelistHistoriesIntake        Kind = "pricelisthistories_intake"
	PricelistHistoriesIntakeV2      Kind = "pricelisthistories_intake_v2"
	PricelistHistoriesComputeIntake Kind = "pricelisthistories_compute_intake"
)
