// Package matching implements the order-to-driver matching consumer. It owns
// the dedicated "fast-matching" consumer group: it selects an available driver
// for each requested order and records an offer so the driver BFF can accept
// it. Persisting offers in PostgreSQL keeps matching consistent with the
// acceptance guard (the same order_acceptances flow) and survives consumer
// restarts.
package matching

import (
	"context"
	"time"

	"uuid"

	"github.com/guilhermelinosp/fast-platform-modular/internal/platform"
	"github.com/guilhermelinosp/hellnet-lib-cache/cache"
)

// Repository is the matching persistence port.
type Repository interface {
	Match(context.Context, string) (Offer, error)
}

// Service matches rides to available drivers. Results are cached per order id
// via GetOrSet (stampede-protected), so Kafka at-least-once reprocessing of
// the same order does not re-run the match and cache misses coalesce.
type Service struct {
	repository Repository
	cache      cache.Cache
}

// NewService creates a matching service.
func NewService(repository Repository, c cache.Cache) *Service {
	return &Service{repository: repository, cache: c}
}

// Match assigns the next available driver to an order. It rejects non-UUID order
// ids and propagates repository outcomes unchanged so consumers can decide
// how to handle no-driver or already-matched cases. Successful matches are
// cached for 5 minutes.
func (s *Service) Match(ctx context.Context, orderID string) (OfferOutput, error) {
	if _, err := uuid.Parse(orderID); err != nil {
		return OfferOutput{}, platform.ValidationError("order_id", "must be a UUID")
	}

	var out OfferOutput
	if s.cache == nil {
		offer, err := s.repository.Match(ctx, orderID)
		if err != nil {
			return OfferOutput{}, err
		}
		return OfferOutput(offer), nil
	}

	err := s.cache.GetOrSet("matching:offer:"+orderID, &out, func(context.Context) (any, error) {
		offer, err := s.repository.Match(ctx, orderID)
		if err != nil {
			return nil, err
		}
		return OfferOutput(offer), nil
	}, 5*time.Minute)
	if err != nil {
		return OfferOutput{}, err
	}
	return out, nil
}
