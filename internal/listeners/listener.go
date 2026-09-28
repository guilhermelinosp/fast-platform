package listeners

import (
	"context"
	"sync"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-database/database"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// outboxStore reads pending outbox rows and records publication audit rows.
type outboxStore interface {
	QueryRow(id string) (Event, bool, error)
	QueryPending() ([]Event, error)
	RecordPublished(id string) error
	RecordFailure(id string, reason string) error
}

// Database is the PostgreSQL-backed outboxStore.
type Database struct{ db *database.DB }

// QueryRow returns a single pending outbox event by id.
func (s Database) QueryRow(id string) (Event, bool, error) {
	return database.QueryRow[Event](s.db, `SELECT id, event_type, event_version, payload FROM outbox_events WHERE id = $1`, id)
}

// QueryPending returns events that have no successful publication yet.
// Tables are append-only (INSERT/SELECT only), so "already published" is
// derived from outbox_publications instead of mutating outbox_events.
func (s Database) QueryPending() ([]Event, error) {
	return database.Query[Event](s.db, `SELECT id, event_type, event_version, payload FROM outbox_events e WHERE NOT EXISTS (SELECT 1 FROM outbox_publications p WHERE p.event_id = e.id) ORDER BY occurred_at LIMIT 100`)
}

// RecordPublished audits a successfully published event. The outbox_events row
// itself is never updated — the NOT EXISTS guard in QueryPending is what
// prevents re-publication.
func (s Database) RecordPublished(id string) error {
	_, err := s.db.Execute(`INSERT INTO outbox_publications (id, event_id, published_at) SELECT gen_random_uuid(), $1, now() WHERE NOT EXISTS (SELECT 1 FROM outbox_publications WHERE event_id = $1)`, id)
	return err
}

// RecordFailure audits a failed event publication.
func (s Database) RecordFailure(id string, reason string) error {
	_, err := s.db.Execute(`INSERT INTO outbox_publication_failures (id, event_id, error, failed_at) VALUES (gen_random_uuid(), $1, $2, now())`, id, reason)
	return err
}

// Event is the durable event envelope stored in PostgreSQL.
type Event struct {
	ID           string `db:"id"`
	EventType    string `db:"event_type"`
	EventVersion int    `db:"event_version"`
	Payload      []byte `db:"payload"`
}

const (
	outboxChannel  = "outbox_events"
	memoMaxEvents  = 4096
	outboxReloadAt = 5 * time.Second
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
	Publish(event Event) error
}

// Listener manages the PostgreSQL NOTIFY listener and reconciliation loop.
type Listener struct {
	store        outboxStore
	publisher    eventPublisher
	listenerConn *database.Conn
	stopListen   func() error
	stopScan     chan struct{}
	workers      sync.WaitGroup
	closeOnce    sync.Once
	memo         *outboxMemo
	ops          telemetry.Client
}

// NewListener starts a PostgreSQL listener for outbox events.
func NewListener(tel telemetry.Client, db *database.DB, publisher eventPublisher) (*Listener, error) {
	conn, err := db.Acquire()
	if err != nil {
		return nil, err
	}
	l := &Listener{
		store:        Database{db: db},
		publisher:    publisher,
		listenerConn: conn,
		stopScan:     make(chan struct{}),
		memo:         newOutboxMemo(),
		ops:          tel,
	}
	stop, err := conn.ListenWithReconnect(outboxChannel, l.onNotification, database.ListenOptions{})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	l.stopListen = stop
	l.workers.Add(1)
	go l.reconcileLoop()
	return l, nil
}

func (l *Listener) onNotification(id string) {
	l.workers.Go(func() {
		if !l.memo.start(id) {
			return
		}
		event, found, err := l.store.QueryRow(id)
		if err != nil {
			l.memo.release(id)
			l.ops.Error("outbox select failed", "error", err, "event_id", id)
			l.incrementCounter("outbox.select.errors.total")
			return
		}
		if !found {
			l.memo.release(id)
			return
		}
		if err := l.publishWithSpan(event); err != nil {
			if auditErr := l.store.RecordFailure(id, err.Error()); auditErr != nil {
				l.ops.Error("outbox failure audit insert failed", "error", auditErr, "event_id", id)
			}
			l.memo.release(id)
			l.ops.Error("outbox publish failed; will retry on next reconcile", "error", err, "event_id", id)
			l.incrementCounter("outbox.publish.errors.total")
			return
		}
		if auditErr := l.store.RecordPublished(id); auditErr != nil {
			l.ops.Error("outbox publication audit insert failed", "error", auditErr, "event_id", id)
		}
		l.memo.settle(id)
		l.incrementCounter("outbox.events.published.total")
	})
}

// publishWithSpan publica o evento dentro de um span de outbox. O producer
// Kafka cria o span filho kafka.publish e propaga o traceparent no header.
func (l *Listener) publishWithSpan(event Event) error {
	if l.ops == nil {
		return l.publisher.Publish(event)
	}
	return l.ops.WithSpan("outbox.publish", func(ctx context.Context) error {
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(attribute.String("event_type", event.EventType))
		return l.publisher.Publish(event)
	})
}

func (l *Listener) incrementCounter(name string) {
	if l.ops != nil {
		if c, err := l.ops.Metric().Counter(name); err == nil {
			c.Add(context.Background(), 1)
		}
	}
}

func (l *Listener) reconcileLoop() {
	defer l.workers.Done()
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
	events, err := l.store.QueryPending()
	if err != nil {
		l.ops.Error("outbox reconciliation failed", "error", err)
		l.incrementCounter("outbox.reconcile.errors.total")
		return
	}
	for _, event := range events {
		if !l.memo.start(event.ID) {
			continue
		}
		if err := l.publishWithSpan(event); err != nil {
			if auditErr := l.store.RecordFailure(event.ID, err.Error()); auditErr != nil {
				l.ops.Error("outbox failure audit insert failed", "error", auditErr, "event_id", event.ID)
			}
			l.memo.release(event.ID)
			l.ops.Error("outbox publish failed; will retry on next reconcile", "error", err, "event_id", event.ID)
			l.incrementCounter("outbox.publish.errors.total")
			continue
		}
		if auditErr := l.store.RecordPublished(event.ID); auditErr != nil {
			l.ops.Error("outbox publication audit insert failed", "error", auditErr, "event_id", event.ID)
		}
		l.memo.settle(event.ID)
		l.incrementCounter("outbox.events.published.total")
	}
}

// Close stops the listener and waits for in-flight publications.
func (l *Listener) Close() {
	l.closeOnce.Do(func() {
		if l.stopListen != nil {
			if err := l.stopListen(); err != nil {
				if l.ops != nil {
					l.ops.Warn("stop outbox listener failed", "error", err)
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
