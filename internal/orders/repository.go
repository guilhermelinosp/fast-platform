package orders

import (
	"context"

	"github.com/guilhermelinosp/hellnet-lib-database/database"
)

// Database persists rider data in PostgreSQL.
type Database struct{ db *database.DB }

// execute runs one SQL statement. It is a package variable so tests can capture
// the statement and its arguments without a database.
var execute = func(ctx context.Context, db *database.DB, sql string, args ...any) (int64, error) {
	return db.ExecuteContext(ctx, sql, args...)
}

// NewRepository creates a rider database repository.
func NewRepository(db *database.DB) *Database {
	return &Database{db: db}
}

// requestedSQL appends the order, its first status and the outbox event in a
// single statement: one round trip, atomic without an explicit transaction, and
// INSERT-only (the tables are append-only).
const requestedSQL = `
WITH o AS (
  INSERT INTO orders (id, rider_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude)
  VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6)
), h AS (
  INSERT INTO order_status_history (id, order_id, sequence, status_id) VALUES ($7::uuid, $1::uuid, 1, 1)
)
INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, payload)
VALUES ($8::uuid, 'order', $1::uuid, $10, 1, $9::jsonb)`

// Requested persists a requested ride and its outbox event.
func (r *Database) Requested(ctx context.Context, input OrderRequestedInput) (Order, error) {
	if _, err := execute(ctx, r.db, requestedSQL,
		input.ID,
		input.RiderID,
		input.PickupLatitude,
		input.PickupLongitude,
		input.DestinationLatitude,
		input.DestinationLongitude,
		input.StatusHistoryID,
		input.OutboxID,
		input.Payload,
		input.EventType); err != nil {
		return Order{}, err
	}
	return Order{
		ID:                   input.ID,
		RiderID:              input.RiderID,
		PickupLatitude:       input.PickupLatitude,
		PickupLongitude:      input.PickupLongitude,
		DestinationLatitude:  input.DestinationLatitude,
		DestinationLongitude: input.DestinationLongitude,
	}, nil
}
