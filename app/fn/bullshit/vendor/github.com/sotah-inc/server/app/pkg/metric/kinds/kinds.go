package kinds

type Kind string

const (
	Collector                       Kind = "collector"
	LiveAuctionsIntake              Kind = "liveauctions_intake"
	LiveAuctionsComputeIntake       Kind = "liveauctions_compute_intake"
	LiveAuctionsIntakeV2            Kind = "liveauctions_intake_v2"
	PricelistHistoriesIntake        Kind = "pricelisthistories_intake"
	PricelistHistoriesIntakeV2      Kind = "pricelisthistories_intake_v2"
	PricelistHistoriesComputeIntake Kind = "pricelisthistories_compute_intake"
)
