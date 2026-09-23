// Command gateway runs the open-banking gateway with an in-memory mock core
// ledger. It is fully local: start it and follow the curl walkthrough in
// README.md to reproduce consent enforcement, idempotent retries and limits.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/consent"
	"github.com/highcumontoa/open-banking-gateway-go/internal/gateway"
	"github.com/highcumontoa/open-banking-gateway-go/internal/httpapi"
	"github.com/highcumontoa/open-banking-gateway-go/internal/idempotency"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ledger"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ratelimit"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	logger := log.New(os.Stdout, "GW ", log.LstdFlags|log.Lmicroseconds)
	now := time.Now

	// ---- Infrastructure (all in-process for local verification) ----
	consentSvc := consent.NewService(consent.NewMemoryStore(), now)
	limiter := ratelimit.NewLimiter(now)
	core := ledger.NewLedger(now)
	idem := idempotency.NewStore(idempotency.JSONFingerprint{})

	// ---- Seed accounts ----
	core.SetAccount("acct-alice", 100_000)
	core.SetAccount("acct-bob", 5_000)

	// ---- Seed deterministic rate/quota rules (per tenant+client) ----
	limiter.SetRule(ratelimit.Subject{TenantID: "tenant-a", ClientID: "client-1"},
		ratelimit.Rule{RatePerMinute: 30, QuotaPerDay: 1000})
	limiter.SetRule(ratelimit.Subject{TenantID: "tenant-b", ClientID: "client-1"},
		ratelimit.Rule{RatePerMinute: 5, QuotaPerDay: 100})
	// Subjects without a rule are open (handy for local experiments).

	gw := gateway.New(limiter, consentSvc, idem, core, logger)
	srv := httpapi.New(gw, consentSvc, limiter, logger)

	logger.Printf("open-banking gateway listening on %s (demo accounts: acct-alice, acct-bob)", *addr)
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatalf("server stopped: %v", err)
	}
}
