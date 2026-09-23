package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/guilhermelinosp/hellnet-lib-environments/environments"
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

func (c *safeConn) WriteMessage(messageType int, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteMessage(messageType, data)
}

func (c *safeConn) ReadMessage() (int, []byte, error) { return c.conn.ReadMessage() }
func (c *safeConn) Close() error                      { return c.conn.Close() }

// connect dials the Engine.IO endpoint and waits for the "0" open packet.
func connectSocket(ops *telemetry.Telemetry, baseURL string) *safeConn {
	url := baseURL + "/socket.io/?EIO=4&transport=websocket"
	ops.Info("Connecting to WebSocket", "url", url)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		ops.Error("Failed to connect", "error", err)
		os.Exit(1)
	}
	// Wait for the Engine.IO open packet ("0{...}") before doing anything.
	_, open, err := conn.ReadMessage()
	if err != nil {
		ops.Error("Failed to read open packet", "error", err)
		os.Exit(1)
	}
	ops.Info("Engine.IO open", "packet", truncate(string(open)))
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
func subscribeToOrder(ops *telemetry.Telemetry, conn *safeConn, namespace string, orderID string) {
	msg := `42` + namespace + `,["order.subscribe","` + orderID + `"]`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		ops.Error("Failed to send subscribe", "error", err)
		return
	}
	ops.Info("RIDER subscribed to order room", "order_id", orderID, "namespace", namespace)
}

// readLoop decodes and logs every incoming packet. When an order.requested
// event arrives on the driver namespace, it extracts the order ID and passes
// it to onOrderRequested so the rider can join the real room.
func readLoop(ops *telemetry.Telemetry, conn *safeConn, label string, onOrderRequested func(orderID string)) {
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				ops.Error("Read error", "label", label, "error", err)
			}
			return
		}
		raw := string(msg)
		// Socket.IO pings are protocol-level text packets ("2"); the server
		// closes the connection if we do not answer with a pong ("3"). Pings
		// are keepalive noise, so we answer silently without logging them.
		if raw == "2" {
			if err := conn.WriteMessage(websocket.TextMessage, []byte("3")); err != nil {
				ops.Error("Failed to send pong", "label", label, "error", err)
			}
			continue
		}
		ops.Info(label+" << packet", "raw", truncate(raw), "decoded", decodePacket(raw))
		if onOrderRequested != nil {
			if orderID := extractOrderRequested(raw); orderID != "" {
				onOrderRequested(orderID)
			}
		}
	}
}

// extractOrderRequested parses a Socket.IO event packet of the form
// 42/ns,["order.requested",{...,"orderId":"..."}] and returns the order ID.
func extractOrderRequested(raw string) string {
	if !strings.HasPrefix(raw, "42") {
		return ""
	}
	_, after, ok := strings.Cut(raw, `["order.requested",`)
	if !ok {
		return ""
	}
	payload := after
	end := strings.LastIndex(payload, "]")
	if end < 0 {
		return ""
	}
	var ev struct {
		OrderID string `json:"orderId"`
	}
	if err := json.Unmarshal([]byte(payload[:end]), &ev); err != nil {
		return ""
	}
	return ev.OrderID
}

// decodePacket turns a Socket.IO v4 packet into a readable description.
func decodePacket(raw string) string {
	if raw == "" {
		return "(empty)"
	}
	switch raw[0] {
	case '0':
		return "open: " + raw[1:]
	case '1':
		return "close"
	case '2':
		return "ping"
	case '3':
		return "pong"
	case '4':
		rest := raw[1:]
		switch {
		case strings.HasPrefix(rest, "0"):
			return "connect namespace: " + rest[1:]
		case strings.HasPrefix(rest, "1"):
			return "disconnect namespace"
		case strings.HasPrefix(rest, "2"):
			return "event: " + rest[1:]
		default:
			return "message: " + rest
		}
	default:
		return raw
	}
}

func truncate(s string) string {
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}

func main() {
	ops, err := telemetry.New()
	if err != nil {
		os.Exit(1)
	}
	defer func() { _ = ops.Close() }()

	baseURL := environments.GetString("SOCKET_URL", "ws://localhost:8080")
	driversNS := environments.GetString("SOCKET_DRIVERS_NAMESPACE", "/drivers")
	ridersNS := environments.GetString("SOCKET_RIDERS_NAMESPACE", "/riders")

	// Driver client
	ops.Info("=== Testing Driver Client ===", "namespace", driversNS, "url", baseURL)
	driverConn := connectSocket(ops, baseURL)
	defer func() { _ = driverConn.Close() }()
	joinNamespace(driverConn, driversNS)

	// Rider client
	ops.Info("=== Testing Rider Client ===", "namespace", ridersNS, "url", baseURL)
	riderConn := connectSocket(ops, baseURL)
	defer func() { _ = riderConn.Close() }()
	joinNamespace(riderConn, ridersNS)

	// When the driver receives an order.requested, subscribe the rider to that
	// real order room so the acceptance notification is delivered.
	go readLoop(ops, driverConn, "DRIVER", func(orderID string) {
		subscribeToOrder(ops, riderConn, ridersNS, orderID)
	})
	go readLoop(ops, riderConn, "RIDER", nil)

	// Keep running to receive events.
	ops.Info("Waiting for events... (Ctrl+C to exit)")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	ops.Info("Shutting down...")
	time.Sleep(1 * time.Second)
}
