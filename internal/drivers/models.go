package drivers

// AcceptedInput contains data needed to accept an order.
type AcceptedInput struct {
	OrderID, DriverID, AcceptanceID, StatusHistoryID, OutboxID string
	Payload                                                    []byte
	EventType                                                  string
}

// Order is the persistence result of a driver operation.
type Order struct{ ID string }

// OrderOutput is the public result of a driver operation.
type OrderOutput struct {
	ID string `json:"id"`
}
