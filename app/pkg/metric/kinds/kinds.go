package kinds

type Kind string

const (
	Collector                Kind = "collector"
	LiveAuctionsIntake       Kind = "liveauctions_intake"
	PricelistHistoriesIntake Kind = "pricelisthistories_intake"
)
