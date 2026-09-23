// Package sockets exposes the Socket.IO transport used by mobile clients.
package sockets

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/guilhermelinosp/fast-platform-modular/internal/orders"
	"github.com/guilhermelinosp/hellnet-lib-environments/environments"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"github.com/zishang520/socket.io/servers/socket/v3"
)

// Server is the mobile Socket.IO gateway. Kafka consumers emit durable ride
// events through it; it does not make outbound HTTP webhook calls.
type Server struct {
	io      *socket.Server
	drivers socket.Namespace
	riders  socket.Namespace
	ops     telemetry.Client
}

// NewServer creates a Socket.IO v4+ server. Mobile driver applications connect
// to /drivers; rider applications connect to /riders and subscribe to their
// order room with the "order.subscribe" event.
func NewServer(ops telemetry.Client) *Server {
	io := socket.NewServer(nil, nil)
	drivers := io.Of(environments.GetString("SOCKET_DRIVERS_NAMESPACE", ""), nil)
	riders := io.Of(environments.GetString("SOCKET_RIDERS_NAMESPACE", ""), nil)

	_ = riders.On("connection", func(args ...any) {
		client, ok := args[0].(*socket.Socket)
		if !ok {
			return
		}
		_ = client.On("order.subscribe", func(values ...any) {
			if len(values) != 1 {
				return
			}
			orderID := toString(values[0])
			if strings.TrimSpace(orderID) == "" {
				return
			}
			client.Join(socket.Room(orderRoom(orderID)))
			if ops != nil {
				ops.Info("socket.order.subscribe", "order_id", orderID, "room", orderRoom(orderID))
			}
		})
	})

	return &Server{io: io, drivers: drivers, riders: riders, ops: ops}
}

// Handler serves the Socket.IO Engine.IO endpoint at /socket.io/.
func (s *Server) Handler() http.Handler { return s.io.ServeHandler(nil) }

// EmitRequested broadcasts an order request to connected driver applications.
func (s *Server) EmitRequested(event orders.OrderRequested) error {
	if s.ops == nil {
		return s.drivers.Emit(environments.GetString("KAFKA_TOPIC_ORDER_REQUESTED", ""), event)
	}
	return s.ops.WithSpan("socket.emit.order_requested", func(ctx context.Context) error {
		s.ops.Info("socket.emit.order_requested",
			"order_id", event.OrderID,
			"rider_id", event.RiderID,
			"event_id", event.EventID,
			"event_version", event.EventVersion,
		)
		return s.drivers.Emit(environments.GetString("KAFKA_TOPIC_ORDER_REQUESTED", ""), event)
	})
}

// EmitAccepted sends acceptance to the mobile client subscribed to this order.
func (s *Server) EmitAccepted(event orders.OrderAccepted) error {
	if s.ops == nil {
		return s.riders.To(socket.Room(orderRoom(event.OrderID))).Emit(environments.GetString("KAFKA_TOPIC_ORDER_ACCEPTED", ""), event)
	}
	return s.ops.WithSpan("socket.emit.order_accepted", func(ctx context.Context) error {
		s.ops.Info("socket.emit.order_accepted",
			"order_id", event.OrderID,
			"driver_id", event.DriverID,
			"event_id", event.EventID,
			"event_version", event.EventVersion,
			"room", orderRoom(event.OrderID),
		)
		return s.riders.To(socket.Room(orderRoom(event.OrderID))).Emit(environments.GetString("KAFKA_TOPIC_ORDER_ACCEPTED", ""), event)
	})
}

func orderRoom(orderID string) string { return "order:" + orderID }

// toString normalizes a Socket.IO event argument into a string. Payloads can
// arrive as string, []byte, json.RawMessage or a Stringer, depending on the
// client parser, so we accept them all.
func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case fmt.Stringer:
		return t.String()
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}
