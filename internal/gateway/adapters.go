package gateway

// The concrete types ratelimit.Limiter, idempotency.Store and ledger.Ledger
// already satisfy the gateway's port interfaces (RateGate, IDStore, Poster),
// so no wrapper types are needed. This file is kept as the documented home of
// that wiring contract; see the constructor in gateway.go and cmd/gateway.
