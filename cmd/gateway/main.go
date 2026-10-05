// gateway 演示本地可运行的开放银行网关：建立同意、执行写入、
// 演示撤销/过期/越权/幂等回放/限流等场景的判定结果。
package main

import (
	"fmt"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/consent"
	"github.com/highcumontoa/open-banking-gateway-go/internal/gateway"
	"github.com/highcumontoa/open-banking-gateway-go/internal/idempotency"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ledger"
	"github.com/highcumontoa/open-banking-gateway-go/internal/quota"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ratelimit"
)

func main() {
	now := time.Now()
	store := consent.NewMemoryStore()
	must(store.Create(consent.Consent{
		ID: "c-1", TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: now.Add(time.Hour),
	}))
	must(store.Create(consent.Consent{
		ID: "c-expired", TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: now.Add(-time.Minute),
	}))
	// 带周期额度的同意：每自然周最多 2 笔。
	must(store.Create(consent.Consent{
		ID: "c-quota", TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: now.Add(time.Hour),
		Quota: &quota.Limit{Period: quota.PeriodWeekly, MaxCount: 2},
	}))

	gw := gateway.New(
		consent.NewAuthorizer(store, time.Now),
		idempotency.NewMemoryStore(),
		ratelimit.NewLimiter(ratelimit.Limit{Requests: 3, Window: time.Minute}, time.Now),
		quota.NewManager(time.Now),
		ledger.NewMock(),
		time.Now,
	)

	run := func(name string, req gateway.Request) {
		resp, err := gw.HandleWrite(req)
		if err != nil {
			fmt.Printf("[%-22s] REJECTED %v\n", name, err)
			return
		}
		fmt.Printf("[%-22s] OK entry=%s replayed=%v\n", name, resp.Entry.ID, resp.Replayed)
	}

	base := gateway.Request{
		TenantID: "tenant-a", ClientID: "client-1", ConsentID: "c-1",
		RequiredScopes: []string{"payment:write"}, IdempotencyKey: "k-1",
		AccountID: "acct-1", Amount: 1000, Currency: "CNY", Memo: "demo",
	}

	run("first write", base)
	run("idempotent replay", base)
	bad := base
	bad.Amount = 9999
	run("key reuse conflict", bad)
	missing := base
	missing.ConsentID = "nope"
	missing.IdempotencyKey = "k-2"
	run("missing consent", missing)
	cross := base
	cross.TenantID = "tenant-b"
	cross.IdempotencyKey = "k-3"
	run("cross-tenant", cross)
	expired := base
	expired.ConsentID = "c-expired"
	expired.IdempotencyKey = "k-4"
	run("expired consent", expired)
	scope := base
	scope.RequiredScopes = []string{"account:read"}
	scope.IdempotencyKey = "k-5"
	run("scope insufficient", scope)

	for i := 0; i < 3; i++ {
		r := base
		r.IdempotencyKey = fmt.Sprintf("k-rl-%d", i)
		run(fmt.Sprintf("rate limit #%d", i+1), r)
	}

	must(store.Revoke("c-1"))
	revoked := base
	revoked.IdempotencyKey = "k-6"
	run("revoked consent", revoked)

	// 同意级周期额度：每周 2 笔，第 3 笔被挡下并给出剩余与重置时间。
	// 换用独立客户端维度，避免与上面的限流演示共用配额。
	for i := 0; i < 3; i++ {
		r := base
		r.ConsentID = "c-quota"
		r.ClientID = "client-2"
		r.IdempotencyKey = fmt.Sprintf("k-quota-%d", i+1)
		run(fmt.Sprintf("weekly quota #%d", i+1), r)
	}
	if st, err := gw.QuotaStatus("tenant-a", "c-quota"); err == nil && st != nil {
		fmt.Printf("[%-22s] used=%d remaining=%d reset_at=%s\n", "quota status",
			st.UsedCount, st.RemainingCount, st.ResetAt.UTC().Format(time.RFC3339))
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
