// Package httpapi exposes the gateway over a small local HTTP API so the full
// consent + idempotency + rate-limit pipeline can be exercised with curl.
//
// Endpoints (all JSON):
//
//	POST /consents                 establish a consent
//	POST /consents/{id}/revoke    revoke a consent
//	POST /payments                 initiate a payment (write, idempotent)
//	GET  /accounts/{id}?consentId= read an account summary
//	GET  /debug/counters           inspect rate-limit windows
//
// Error responses always use the shape {"code":..., "message":...} with a
// stable domain error code; raw internal errors are never serialized.
package httpapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/domain"
	"github.com/highcumontoa/open-banking-gateway-go/internal/gateway"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ratelimit"
)

type Server struct {
	gateway *gateway.Gateway
	consent ConsentService
	rates   RateInspector
	logger  *log.Logger
	mux     *http.ServeMux
}

// ConsentService is the consent lifecycle surface used by HTTP handlers.
type ConsentService interface {
	Create(in domain.CreateConsentInput) (domain.Consent, error)
	Revoke(id domain.ConsentID) (domain.Consent, error)
}

// RateInspector lets the debug endpoint render window counters.
type RateInspector interface {
	SetRule(s ratelimit.Subject, r ratelimit.Rule)
	Snapshot(s ratelimit.Subject) ratelimit.Counters
}

func New(gw *gateway.Gateway, consents ConsentService, rates RateInspector, logger *log.Logger) *Server {
	s := &Server{gateway: gw, consent: consents, rates: rates, logger: logger, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("POST /consents", s.handleCreateConsent)
	s.mux.HandleFunc("POST /consents/{id}/revoke", s.handleRevoke)
	s.mux.HandleFunc("POST /payments", s.handlePayment)
	s.mux.HandleFunc("GET /accounts/{id}", s.handleReadAccount)
	s.mux.HandleFunc("GET /debug/counters", s.handleCounters)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "time": time.Now().Format(time.RFC3339)})
	})
}

type createConsentRequest struct {
	TenantID  string         `json:"tenantId"`
	ClientID  string         `json:"clientId"`
	AccountID string         `json:"accountId"`
	Scopes    []domain.Scope `json:"scopes"`
	TTLHours  int64          `json:"ttlHours"`
}

func (s *Server) handleCreateConsent(w http.ResponseWriter, r *http.Request) {
	var req createConsentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, domain.NewError(domain.ErrInvalidRequest, "invalid JSON body"))
		return
	}
	ttl := time.Duration(req.TTLHours) * time.Hour
	if ttl <= 0 {
		ttl = time.Hour
	}
	c, err := s.consent.Create(domain.CreateConsentInput{
		TenantID:  domain.TenantID(req.TenantID),
		ClientID:  domain.ClientID(req.ClientID),
		AccountID: domain.AccountID(req.AccountID),
		Scopes:    req.Scopes,
		TTL:       ttl,
	})
	if err != nil {
		writeDomainError(w, err)
		return
	}
	s.logger.Printf("HTTP consent created id=%s tenant=%s client=%s account=%s scopes=%v ttl=%s",
		c.ID, c.TenantID, c.ClientID, c.AccountID, c.Scopes, ttl)
	writeJSON(w, http.StatusCreated, c)
}

func (s *Server) handleRevoke(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	c, err := s.consent.Revoke(domain.ConsentID(id))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	s.logger.Printf("HTTP consent revoked id=%s", id)
	writeJSON(w, http.StatusOK, c)
}

type paymentRequest struct {
	IdempotencyKey string       `json:"idempotencyKey"`
	TenantID       string       `json:"tenantId"`
	ClientID       string       `json:"clientId"`
	ConsentID      string       `json:"consentId"`
	AccountID      string       `json:"accountId"`
	Amount         domain.Money `json:"amount"`
	Reference      string       `json:"reference"`
}

func (s *Server) handlePayment(w http.ResponseWriter, r *http.Request) {
	var req paymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, domain.NewError(domain.ErrInvalidRequest, "invalid JSON body"))
		return
	}
	// The idempotency key may alternatively arrive via header (common pattern).
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	}
	in := domain.PaymentInput{
		IdempotencyKey: req.IdempotencyKey,
		TenantID:       domain.TenantID(req.TenantID),
		ClientID:       domain.ClientID(req.ClientID),
		ConsentID:      domain.ConsentID(req.ConsentID),
		AccountID:      domain.AccountID(req.AccountID),
		Amount:         req.Amount,
		Reference:      req.Reference,
	}
	res, err := s.gateway.SubmitPayment(r.Context(), in)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	status := http.StatusOK
	if !res.Replayed {
		status = http.StatusCreated
	}
	writeJSON(w, status, res)
}

func (s *Server) handleReadAccount(w http.ResponseWriter, r *http.Request) {
	tenantID := domain.TenantID(r.URL.Query().Get("tenantId"))
	clientID := domain.ClientID(r.URL.Query().Get("clientId"))
	consentID := domain.ConsentID(r.URL.Query().Get("consentId"))
	accountID := domain.AccountID(r.PathValue("id"))

	summary, err := s.gateway.ReadAccount(r.Context(), tenantID, clientID, consentID, accountID)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"account": string(accountID), "summary": summary})
}

func (s *Server) handleCounters(w http.ResponseWriter, r *http.Request) {
	subj := ratelimit.Subject{
		TenantID: domain.TenantID(r.URL.Query().Get("tenantId")),
		ClientID: domain.ClientID(r.URL.Query().Get("clientId")),
	}
	writeJSON(w, http.StatusOK, s.rates.Snapshot(subj))
}

// ---- response helpers ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, e *domain.GatewayError) {
	writeJSON(w, status, e)
}

// httpStatusForCode maps stable domain codes to HTTP statuses.
func httpStatusForCode(c domain.ErrorCode) int {
	switch c {
	case domain.ErrInvalidRequest:
		return http.StatusBadRequest
	case domain.ErrConsentMissing:
		return http.StatusNotFound
	case domain.ErrConsentRevoked, domain.ErrConsentExpired,
		domain.ErrScopeInsufficient, domain.ErrCrossTenantAccess,
		domain.ErrClientMismatch, domain.ErrAccountMismatch:
		return http.StatusForbidden
	case domain.ErrIdempotencyConflict:
		return http.StatusConflict
	case domain.ErrRateLimited, domain.ErrQuotaExceeded:
		return http.StatusTooManyRequests
	case domain.ErrUpstreamTimeout:
		return http.StatusGatewayTimeout
	case domain.ErrUpstreamUnavailable, domain.ErrUpstreamRejected:
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}

func writeDomainError(w http.ResponseWriter, err error) {
	var ge *domain.GatewayError
	if !errors.As(err, &ge) {
		ge = domain.NewError(domain.ErrInternal, "unclassified failure")
	}
	// Strip any trailing carriage-return noise defensively.
	ge.Message = strings.TrimSpace(ge.Message)
	writeJSON(w, httpStatusForCode(ge.Code), ge)
}
