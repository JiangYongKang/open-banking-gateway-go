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
	// 带周期额度的同意：每周最多 2 笔（笔数上限）。
	must(store.Create(consent.Consent{
		ID: "c-quota", TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: now.Add(time.Hour),
		Quota: quota.Policy{Period: quota.PeriodWeekly, MaxCount: 2},
	}))

	// 仅金额上限的同意：每周累计不超过 1500 CNY，不限笔数。
	must(store.Create(consent.Consent{
		ID: "c-quota-amt", TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: now.Add(time.Hour),
		Quota: quota.Policy{Period: quota.PeriodWeekly, MaxAmount: 1500, Currency: "CNY"},
	}))

	gw := gateway.New(
		consent.NewAuthorizer(store, time.Now),
		idempotency.NewMemoryStore(),
		ratelimit.NewLimiter(ratelimit.Limit{Requests: 5, Window: time.Minute}, time.Now),
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

	// 同意级周期额度：独立 client 维度，先于限流演示，避免被限流桶干扰。
	// 笔数上限：每周 2 笔。
	q1 := base
	q1.ClientID = "client-q"
	q1.ConsentID = "c-quota"
	q1.Amount = 1000
	q1.IdempotencyKey = "k-q-1"
	run("quota write #1", q1)
	q2 := q1
	q2.IdempotencyKey = "k-q-2"
	run("quota write #2", q2)
	q3 := q1
	q3.IdempotencyKey = "k-q-3"
	run("quota write #3 (exhaust)", q3)
	qretry := q1
	qretry.IdempotencyKey = "k-q-1"
	run("quota idempotent replay", qretry)

	// 金额预算：每周累计不超过 1500 CNY；1000 放行，再发 600 被额度挡下。
	a1 := base
	a1.ClientID = "client-q2"
	a1.ConsentID = "c-quota-amt"
	a1.Amount = 1000
	a1.IdempotencyKey = "k-a-1"
	run("amount quota write #1", a1)
	a2 := a1
	a2.Amount = 600
	a2.IdempotencyKey = "k-a-2"
	run("amount quota write #2 (over)", a2)

	// 限流用独立 client 维度演示：窗口内前 5 笔放行，第 6 笔被限流。
	for i := 0; i < 6; i++ {
		r := base
		r.ClientID = "client-rl"
		r.IdempotencyKey = fmt.Sprintf("k-rl-%d", i)
		run(fmt.Sprintf("rate limit #%d", i+1), r)
	}

	must(store.Revoke("c-1"))
	revoked := base
	revoked.IdempotencyKey = "k-6"
	run("revoked consent", revoked)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
