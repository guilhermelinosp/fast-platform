package matching

import (
	"context"

	"github.com/guilhermelinosp/fast-platform/platform"
	"github.com/guilhermelinosp/hellnet-lib-database/database"
)

// availableDriverRow maps the driver selected by the matching query.
type availableDriverRow struct {
	DriverID string `db:"driver_id"`
}

// execFn is a single SQL execution inside a transaction. It is the unit the
// repository depends on, so tests can capture statements without a database.
type execFn func(sql string, args ...any) (int64, error)

// driverFn selects the available driver inside the same transaction. Tests
// replace it with a canned result.
type driverFn func() (string, bool, error)

// transactional runs fn inside a database transaction, handing it the execute
// seam and a driver-selection closure built on the same transaction. Package
// variable so tests can replace it with a fake.
var transactional = func(ctx context.Context, db *database.DB, fn func(execute execFn, driver driverFn) error) error {
	return db.TransactionalContext(ctx, func(ctx context.Context, tx *database.Tx) error {
		execute := func(sql string, args ...any) (int64, error) { return tx.ExecuteContext(ctx, sql, args...) }
		return fn(execute, func() (string, bool, error) {
			row, found, err := database.TxQueryRowContext[availableDriverRow](ctx, tx, `
SELECT e.driver_id
FROM driver_availability_history e
JOIN driver_availability_statuses s ON s.id = e.availability_status_id
LEFT JOIN driver_availability_history newer ON newer.driver_id = e.driver_id AND (newer.occurred_at > e.occurred_at OR (newer.occurred_at = e.occurred_at AND newer.sequence > e.sequence))
WHERE s.code = 'online' AND newer.driver_id IS NULL
ORDER BY e.occurred_at, e.sequence
LIMIT 1`)
			if err != nil || !found {
				return "", found, err
			}
			return row.DriverID, true, nil
		})
	})
}

// Database persists ride offers in PostgreSQL.
type Database struct{ db *database.DB }

// NewRepository creates a matching repository.
func NewRepository(db *database.DB) *Database { return &Database{db: db} }

// Match assigns an available driver to an order, recording the offer. Matching
// is idempotent: an order with an existing offer is reported via
// ErrRideAlreadyMatched without inserting a duplicate.
func (r *Database) Match(ctx context.Context, orderID string) (Offer, error) {
	var offer Offer
	err := transactional(ctx, r.db, func(execute execFn, driver driverFn) error {
		driverID, found, err := driver()
		if err != nil {
			return err
		}
		if !found {
			return platform.NewError(503, "NO_DRIVER_AVAILABLE", "matching: no driver available")
		}
		n, err := execute(`INSERT INTO ride_offers (id, ride_id, driver_id, status, created_at) SELECT gen_random_uuid(), $1, $2, 'pending', now() WHERE NOT EXISTS (SELECT 1 FROM ride_offers WHERE ride_id = $1)`, orderID, driverID)
		if err != nil {
			return err
		}
		if n == 0 {
			return platform.NewError(409, "RIDE_ALREADY_MATCHED", "matching: ride already matched")
		}
		offer = Offer{OrderID: orderID, DriverID: driverID, Status: "pending"}
		return nil
	})
	return offer, err
}
