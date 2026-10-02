// Package events holds the order events published on Kafka. It is public so the services that
// produce and consume them (fast-platform, fast-listeners, fast-sockets) share one definition.
package events

import "github.com/guilhermelinosp/fast-platform/env"

// OrderRequested is the Kafka event emitted when an order is requested.
type OrderRequested struct {
	EventID              string  `json:"eventId" avro:"eventId"`
	EventVersion         int     `json:"eventVersion" avro:"eventVersion"`
	OccurredAt           int64   `json:"occurredAt" avro:"occurredAt"`
	OrderID              string  `json:"orderId" avro:"orderId"`
	RiderID              string  `json:"riderId" avro:"riderId"`
	PickupLatitude       float64 `json:"pickupLatitude" avro:"pickupLatitude"`
	PickupLongitude      float64 `json:"pickupLongitude" avro:"pickupLongitude"`
	DestinationLatitude  float64 `json:"destinationLatitude" avro:"destinationLatitude"`
	DestinationLongitude float64 `json:"destinationLongitude" avro:"destinationLongitude"`
}

// MessageType returns the Kafka topic for order-requested events.
func (OrderRequested) MessageType() string {
	return env.String("KAFKA_TOPIC_ORDER_REQUESTED", "")
}

// OrderAccepted is the Kafka event emitted when an order is accepted.
type OrderAccepted struct {
	EventID      string `json:"eventId"`
	EventVersion int    `json:"eventVersion"`
	OccurredAt   int64  `json:"occurredAt"`
	OrderID      string `json:"orderId"`
	DriverID     string `json:"driverId"`
}

// MessageType returns the order accepted event type for Kafka (no error).
// Satisfies kafka.Message interface.
func (OrderAccepted) MessageType() string {
	return env.String("KAFKA_TOPIC_ORDER_ACCEPTED", "")
}
