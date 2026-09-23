package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/highcumontoa/open-banking-gateway-go/internal/domain"
)

// JSONFingerprint fingerprints a payment by its business content.
//
// The idempotency key is deliberately excluded: two submissions with the same
// key MUST carry the same business content. Struct field order is fixed by the
// Go type system, so encoding/json output is deterministic without map sorting.
type JSONFingerprint struct{}

func (JSONFingerprint) Fingerprint(in domain.PaymentInput) (string, error) {
	canonical := struct {
		TenantID  domain.TenantID
		ClientID  domain.ClientID
		ConsentID domain.ConsentID
		AccountID domain.AccountID
		Amount    domain.Money
		Reference string
	}{
		TenantID:  in.TenantID,
		ClientID:  in.ClientID,
		ConsentID: in.ConsentID,
		AccountID: in.AccountID,
		Amount:    in.Amount,
		Reference: in.Reference,
	}
	b, err := json.Marshal(canonical)
	if err != nil {
		return "", domain.NewError(domain.ErrInternal, "failed to fingerprint request")
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
