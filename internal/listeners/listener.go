package listeners

import (
	"context"
	"sync"
	"time"

	"github.com/guilhermelinosp/fast-platform/platform"
	"github.com/guilhermelinosp/hellnet-lib-database/database"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// outboxStore reads pending outbox rows and records publication audit rows.
type outboxStore interface {
	QueryRow(ctx context.Context, id string) (Event, bool, error)
	QueryPending(ctx context.Context, since time.Time) ([]Event, error)
	RecordPublished(ctx context.Context, id string) error
	RecordFailure(ctx context.Context, id string, reason string) error
}

// Database is the PostgreSQL-backed outboxStore.
type Database struct{ db *database.DB }

// QueryRow returns a single pending outbox event by id.
func (s Database) QueryRow(ctx context.Context, id string) (Event, bool, error) {
	return database.QueryRowContext[Event](ctx, s.db, `SELECT id, aggregate_id, event_type, event_version, payload FROM outbox_events WHERE id = $1`, id)
}

// QueryPending returns events since `since` that have no successful publication
// yet. Tables are append-only (INSERT/SELECT only), so "already published" is
// derived from outbox_publications instead of mutating outbox_events. The
// occurred_at bound keeps the anti-join proportional to recent events instead of
// the whole, ever-growing table; a zero `since` scans everything.
func (s Database) QueryPending(ctx context.Context, since time.Time) ([]Event, error) {
	return database.QueryContext[Event](ctx, s.db, `SELECT id, aggregate_id, event_type, event_version, payload FROM outbox_events e WHERE e.occurred_at >= $1 AND NOT EXISTS (SELECT 1 FROM outbox_publications p WHERE p.event_id = e.id) ORDER BY occurred_at LIMIT 100`, since)
}

// RecordPublished audits a successfully published event. The outbox_events row
// itself is never updated — the NOT EXISTS guard in QueryPending is what
// prevents re-publication.
func (s Database) RecordPublished(ctx context.Context, id string) error {
	_, err := s.db.ExecuteContext(ctx, `INSERT INTO outbox_publications (id, event_id, published_at) SELECT gen_random_uuid(), $1, now() WHERE NOT EXISTS (SELECT 1 FROM outbox_publications WHERE event_id = $1)`, id)
	return err
}

// RecordFailure audits a failed event publication.
func (s Database) RecordFailure(ctx context.Context, id string, reason string) error {
	_, err := s.db.ExecuteContext(ctx, `INSERT INTO outbox_publication_failures (id, event_id, error, failed_at) VALUES (gen_random_uuid(), $1, $2, now())`, id, reason)
	return err
}

// Event is the durable event envelope stored in PostgreSQL.
type Event struct {
	ID           string `db:"id"`
	AggregateID  string `db:"aggregate_id"`
	EventType    string `db:"event_type"`
	EventVersion int    `db:"event_version"`
	Payload      []byte `db:"payload"`
}

const (
	outboxChannel  = "outbox_events"
	memoMaxEvents  = 4096
	outboxReloadAt = 5 * time.Second
	// outboxLookback bounds the periodic reconcile to recent events; older ones are
	// only picked up by the full scan that runs at startup and every outboxFullScanEvery.
	outboxLookback      = time.Hour
	outboxFullScanEvery = time.Hour
)

// outboxMemo is the in-memory delivery state for the insert/select-only outbox.
type outboxMemo struct {
	mu       sync.Mutex
	inflight map[string]struct{}
	done     map[string]struct{}
	order    []string
}

func newOutboxMemo() *outboxMemo {
	return &outboxMemo{
		inflight: make(map[string]struct{}),
		done:     make(map[string]struct{}),
	}
}

func (m *outboxMemo) start(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.done[id]; ok {
		return false
	}
	if _, ok := m.inflight[id]; ok {
		return false
	}
	m.inflight[id] = struct{}{}
	return true
}

func (m *outboxMemo) release(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inflight, id)
}

func (m *outboxMemo) settle(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inflight, id)
	if _, ok := m.done[id]; ok {
		return
	}
	m.done[id] = struct{}{}
	m.order = append(m.order, id)
	for len(m.order) > memoMaxEvents {
		oldest := m.order[0]
		m.order = m.order[1:]
		delete(m.done, oldest)
	}
}

// eventPublisher is the port the listener uses to publish a single event.
type eventPublisher interface {
	Publish(ctx context.Context, event Event) error
}

// Listener manages the PostgreSQL NOTIFY listener and reconciliation loop.
type Listener struct {
	store        outboxStore
	publisher    eventPublisher
	listenerConn *database.Conn
	stopListen   func() error
	stopScan     chan struct{}
	ctx          context.Context
	workers      sync.WaitGroup
	closeOnce    sync.Once
	memo         *outboxMemo
	ops          *telemetry.Telemetry
	lastFullScan time.Time // touched only by the reconcile loop
}

// NewListener starts a PostgreSQL listener for outbox events.
func NewListener(ctx context.Context, tel *telemetry.Telemetry, db *database.DB, publisher eventPublisher) (*Listener, error) {
	conn, err := db.Acquire()
	if err != nil {
		return nil, err
	}
	l := &Listener{
		store:        Database{db: db},
		publisher:    publisher,
		listenerConn: conn,
		stopScan:     make(chan struct{}),
		ctx:          ctx,
		memo:         newOutboxMemo(),
		ops:          tel,
	}
	stop, err := conn.ListenWithReconnect(outboxChannel, l.onNotification, database.ListenOptions{})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	l.stopListen = stop
	l.workers.Go(func() {
		l.reconcileLoop()
	})
	return l, nil
}

func (l *Listener) onNotification(id string) {
	l.workers.Go(func() {
		if !l.memo.start(id) {
			return
		}
		// Selecting the event happens before its trace context is known, so it
		// is not traced (it would be an orphan trace per event).
		event, found, err := l.store.QueryRow(instrument.WithoutTracing(l.ctx), id)
		if err != nil {
			l.memo.release(id)
			l.logError(l.ctx, "outbox select failed", "error", err, "event_id", id)
			l.incrementCounter("outbox.select.errors.total")
			return
		}
		if !found {
			l.memo.release(id)
			return
		}
		l.process(event)
	})
}

// process delivers one event inside an outbox.publish span that continues the
// trace of the request that wrote it. The publication audit runs in the same
// span, so it is part of the trace instead of an orphan. The caller must have
// claimed the event with memo.start.
func (l *Listener) process(event Event) {
	parent := platform.ExtractTraceContext(l.ctx, event.Payload)
	work := func(ctx context.Context) error {
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("event_id", event.ID),
			attribute.String("event_type", event.EventType),
			attribute.String("order_id", event.AggregateID),
		)
		l.logInfo(ctx, "outbox event received", "event_id", event.ID, "event_type", event.EventType)
		if err := l.publisher.Publish(ctx, event); err != nil {
			if auditErr := l.store.RecordFailure(ctx, event.ID, err.Error()); auditErr != nil {
				l.logError(ctx, "outbox failure audit insert failed", "error", auditErr, "event_id", event.ID)
			}
			return err
		}
		l.logInfo(ctx, "outbox event published to kafka", "event_id", event.ID, "event_type", event.EventType)
		if auditErr := l.store.RecordPublished(ctx, event.ID); auditErr != nil {
			l.logError(ctx, "outbox publication audit insert failed", "error", auditErr, "event_id", event.ID)
		}
		return nil
	}

	var err error
	if l.ops == nil {
		err = work(parent)
	} else {
		err = l.ops.Trace(parent).Span("outbox.publish", work)
	}
	if err != nil {
		l.memo.release(event.ID)
		l.logError(parent, "outbox publish failed; will retry on next reconcile", "error", err, "event_id", event.ID)
		l.incrementCounter("outbox.publish.errors.total")
		return
	}
	l.memo.settle(event.ID)
	l.incrementCounter("outbox.events.published.total")
}

func (l *Listener) logInfo(ctx context.Context, msg string, args ...any) {
	if l.ops != nil {
		l.ops.Log(ctx).Info(msg, args...)
	}
}

func (l *Listener) logError(ctx context.Context, msg string, args ...any) {
	if l.ops != nil {
		l.ops.Log(ctx).Error(msg, args...)
	}
}

func (l *Listener) incrementCounter(name string) {
	if l.ops != nil {
		_ = l.ops.Metric(l.ctx).Counter(name, 1)
	}
}

func (l *Listener) reconcileLoop() {
	ticker := time.NewTicker(outboxReloadAt)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.reconcileOnce()
		case <-l.stopScan:
			return
		}
	}
}

func (l *Listener) reconcileOnce() {
	// Full scan at startup and periodically; the ticks in between only look at
	// recent events so the cost does not grow with the table.
	since, full := time.Now().Add(-outboxLookback), false
	if l.lastFullScan.IsZero() || time.Since(l.lastFullScan) >= outboxFullScanEvery {
		since, full = time.Time{}, true
	}
	// The poll runs every outboxReloadAt; tracing it would create one trace per tick.
	events, err := l.store.QueryPending(instrument.WithoutTracing(l.ctx), since)
	if err == nil && full {
		l.lastFullScan = time.Now()
	}
	if err != nil {
		l.logError(l.ctx, "outbox reconciliation failed", "error", err)
		l.incrementCounter("outbox.reconcile.errors.total")
		return
	}
	for _, event := range events {
		if !l.memo.start(event.ID) {
			continue
		}
		l.process(event)
	}
}

// Close stops the listener and waits for in-flight publications.
func (l *Listener) Close() {
	l.closeOnce.Do(func() {
		if l.stopListen != nil {
			if err := l.stopListen(); err != nil {
				if l.ops != nil {
					l.ops.Log(l.ctx).Warn("stop outbox listener failed", "error", err)
				}
			}
		}
		close(l.stopScan)
		l.workers.Wait()
		if l.listenerConn != nil {
			_ = l.listenerConn.Close()
		}
	})
}
