package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/guilhermelinosp/fast-platform/internal/env"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

// safeConn serializes writes on a websocket connection. gorilla/websocket does
// not allow concurrent WriteMessage calls: the pong reply and the order
// subscribe originate from different goroutines, so without this mutex the
// frames interleave and the server drops the connection.
type safeConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

type socketEvent struct {
	Namespace string
	Name      string
	Payload   json.RawMessage
}

func (c *safeConn) WriteMessage(messageType int, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteMessage(messageType, data)
}

func (c *safeConn) ReadMessage() (int, []byte, error) { return c.conn.ReadMessage() }
func (c *safeConn) Close() error                      { return c.conn.Close() }

// connect dials the Engine.IO endpoint and waits for the "0" open packet.
func connectSocket(ctx context.Context, ops *telemetry.Telemetry, baseURL string) *safeConn {
	url := baseURL + "/socket.io/?EIO=4&transport=websocket"
	ops.Log(ctx).Info("Connecting to WebSocket", "url", url)
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		ops.Log(ctx).Error("Failed to connect", "error", err)
		os.Exit(1)
	}
	// Wait for the Engine.IO open packet ("0{...}") before doing anything.
	_, open, err := conn.ReadMessage()
	if err != nil {
		ops.Log(ctx).Error("Failed to read open packet", "error", err)
		os.Exit(1)
	}
	ops.Log(ctx).Info("Engine.IO open", "packet_type", packetType(string(open)))
	return &safeConn{conn: conn}
}

// joinNamespace sends the Socket.IO connect packet for a namespace.
func joinNamespace(conn *safeConn, namespace string) {
	_ = conn.WriteMessage(websocket.TextMessage, []byte("40"+namespace))
}

// subscribeToOrder joins the rider to the room of a specific order. The event
// must carry the rider namespace explicitly (42<ns>,["order.subscribe",...]);
// without it the packet is routed to the root namespace where no handler
// exists and the rider never enters the room.
func subscribeToOrder(ctx context.Context, ops *telemetry.Telemetry, conn *safeConn, namespace string, orderID string) {
	msg := `42` + namespace + `,["order.subscribe","` + orderID + `"]`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		ops.Log(ctx).Error("Failed to send subscribe", "error", err)
		return
	}
	ops.Log(ctx).Info("RIDER subscribed to order room", "order_id", orderID, "namespace", namespace)
}

// readLoop decodes and logs every incoming packet. When an order.requested
// event arrives on the driver namespace, it extracts the order ID and passes
// it to onOrderRequested so the rider can join the real room.
func readLoop(ctx context.Context, ops *telemetry.Telemetry, conn *safeConn, label, requestedEvent string, onOrderRequested func(orderID string)) {
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				ops.Log(ctx).Error("Read error", "label", label, "error", err)
			}
			return
		}
		raw := string(msg)
		// Socket.IO pings are protocol-level text packets ("2"); the server
		// closes the connection if we do not answer with a pong ("3"). Pings
		// are keepalive noise, so we answer silently without logging them.
		if raw == "2" {
			if err := conn.WriteMessage(websocket.TextMessage, []byte("3")); err != nil {
				ops.Log(ctx).Error("Failed to send pong", "label", label, "error", err)
			}
			continue
		}
		logPacket(ctx, ops, label, raw)
		if onOrderRequested != nil {
			if orderID := extractOrderRequested(raw, requestedEvent); orderID != "" {
				onOrderRequested(orderID)
			}
		}
	}
}

func logPacket(ctx context.Context, ops *telemetry.Telemetry, label, raw string) {
	event, ok := parseSocketEvent(raw)
	if !ok {
		ops.Log(ctx).Info(label+" << packet", "packet_type", packetType(raw))
		return
	}

	fields := []any{"namespace", event.Namespace, "event", event.Name}
	var payload struct {
		EventID  string `json:"eventId"`
		OrderID  string `json:"orderId"`
		DriverID string `json:"driverId"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err == nil {
		if payload.EventID != "" {
			fields = append(fields, "event_id", payload.EventID)
		}
		if payload.OrderID != "" {
			fields = append(fields, "order_id", payload.OrderID)
		}
		if payload.DriverID != "" {
			fields = append(fields, "driver_id", payload.DriverID)
		}
	}
	ops.Log(ctx).Info(label+" << event", fields...)
}

func parseSocketEvent(raw string) (socketEvent, bool) {
	if !strings.HasPrefix(raw, "42") {
		return socketEvent{}, false
	}
	rest := raw[2:]
	comma := strings.IndexByte(rest, ',')
	if comma < 0 {
		return socketEvent{}, false
	}
	var parts []json.RawMessage
	if err := json.Unmarshal([]byte(rest[comma+1:]), &parts); err != nil || len(parts) < 2 {
		return socketEvent{}, false
	}
	var name string
	if err := json.Unmarshal(parts[0], &name); err != nil || name == "" {
		return socketEvent{}, false
	}
	return socketEvent{Namespace: rest[:comma], Name: name, Payload: parts[1]}, true
}

func packetType(raw string) string {
	switch {
	case strings.HasPrefix(raw, "40"):
		return "namespace_connect"
	case strings.HasPrefix(raw, "42"):
		return "event"
	case strings.HasPrefix(raw, "0"):
		return "engine_open"
	case strings.HasPrefix(raw, "1"):
		return "engine_close"
	case strings.HasPrefix(raw, "2"):
		return "ping"
	case strings.HasPrefix(raw, "3"):
		return "pong"
	default:
		return "unknown"
	}
}

// extractOrderRequested parses a Socket.IO event packet of the form
// 42/ns,["<requestedEvent>",{...,"orderId":"..."}] and returns the order ID.
// The server names the event after the order-requested topic, so requestedEvent
// is KAFKA_TOPIC_ORDER_REQUESTED.
func extractOrderRequested(raw, requestedEvent string) string {
	event, ok := parseSocketEvent(raw)
	if !ok || event.Name != requestedEvent {
		return ""
	}
	var ev struct {
		OrderID string `json:"orderId"`
	}
	if err := json.Unmarshal(event.Payload, &ev); err != nil {
		return ""
	}
	return ev.OrderID
}

// runClient is the Socket.IO test client: `go run . client` in cmd/sockets. It
// connects as a driver and as a rider, logs every packet and subscribes the
// rider to the orders the driver receives.
func runClient() {
	ctx := context.Background()
	_ = env.Environment() // load cmd/sockets/.env when present; variables already set win
	ops, err := telemetry.New(ctx)
	if err != nil {
		os.Exit(1)
	}
	defer func() { _ = ops.Close(ctx) }()

	baseURL := env.String("SOCKET_URL", "ws://localhost:8080")
	driversNS := env.String("SOCKET_DRIVERS_NAMESPACE", "/drivers")
	ridersNS := env.String("SOCKET_RIDERS_NAMESPACE", "/riders")
	requestedEvent := env.String("KAFKA_TOPIC_ORDER_REQUESTED", "br.com.hellnet.fast.order.requested.v1")

	// Driver client
	ops.Log(ctx).Info("=== Testing Driver Client ===", "namespace", driversNS, "url", baseURL)
	driverConn := connectSocket(ctx, ops, baseURL)
	defer func() { _ = driverConn.Close() }()
	joinNamespace(driverConn, driversNS)

	// Rider client
	ops.Log(ctx).Info("=== Testing Rider Client ===", "namespace", ridersNS, "url", baseURL)
	riderConn := connectSocket(ctx, ops, baseURL)
	defer func() { _ = riderConn.Close() }()
	joinNamespace(riderConn, ridersNS)

	// When the driver receives an order.requested, subscribe the rider to that
	// real order room so the acceptance notification is delivered.
	go readLoop(ctx, ops, driverConn, "DRIVER", requestedEvent, func(orderID string) {
		subscribeToOrder(ctx, ops, riderConn, ridersNS, orderID)
	})
	go readLoop(ctx, ops, riderConn, "RIDER", requestedEvent, nil)

	// Keep running to receive events.
	ops.Log(ctx).Info("Waiting for events... (Ctrl+C to exit)")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	ops.Log(ctx).Info("Shutting down...")
	time.Sleep(1 * time.Second)
}
