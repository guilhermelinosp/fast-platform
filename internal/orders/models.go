package orders

import (
	"time"
)

// Order is the persisted order representation returned by the service.
type Order struct {
	ID, RiderID                                                                string
	PickupLatitude, PickupLongitude, DestinationLatitude, DestinationLongitude float64
}

// OrderRequestedInput contains data needed to request an order.
type OrderRequestedInput struct {
	ID, RiderID                                                                string
	PickupLatitude, PickupLongitude, DestinationLatitude, DestinationLongitude float64
	StatusHistoryID, OutboxID                                                  string
	Payload                                                                    []byte
	EventType                                                                  string
}

// OrderOutput is the public order response.
type OrderOutput struct {
	ID                   string  `json:"id"`
	RiderID              string  `json:"rider_id"`
	PickupLatitude       float64 `json:"pickup_latitude"`
	PickupLongitude      float64 `json:"pickup_longitude"`
	DestinationLatitude  float64 `json:"destination_latitude"`
	DestinationLongitude float64 `json:"destination_longitude"`
}

// OrderView is the read model of an order: its data and current status (the
// last row of order_status_history). It is also what the cache stores.
type OrderView struct {
	ID                   string    `db:"id" json:"id"`
	RiderID              string    `db:"rider_id" json:"rider_id"`
	PickupLatitude       float64   `db:"pickup_latitude" json:"pickup_latitude"`
	PickupLongitude      float64   `db:"pickup_longitude" json:"pickup_longitude"`
	DestinationLatitude  float64   `db:"destination_latitude" json:"destination_latitude"`
	DestinationLongitude float64   `db:"destination_longitude" json:"destination_longitude"`
	Status               string    `db:"status" json:"status"`
	CreatedAt            time.Time `db:"created_at" json:"created_at"`
}
