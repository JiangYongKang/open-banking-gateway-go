# Open Banking Gateway（本地可运行，Go）

一套**完全本地**运行的开放银行接口网关与同意（consent）授权流程演示：以进程内
内存存储承载同意与幂等记录，以模拟的核心账务服务（mock core ledger）承载写入，
让不同**客户端**与**租户**在各类同意状态下得到**一致且可解释**的结论。

无任何外部依赖（仅 Go 标准库），`go run` 即可启动，`curl` 即可完成全流程验证。

## 目录结构

```
cmd/gateway/            本地 HTTP 服务入口（组装与种子数据）
internal/domain/        领域类型与统一错误码（拒绝原因词汇表）
internal/consent/       同意生命周期：建立 / 撤销 / 到期 / 鉴权判定
internal/idempotency/   幂等存储：in-flight 锁、稳定回放、指纹冲突
internal/ratelimit/     固定分钟窗口限流 + UTC 日历日配额
internal/ledger/        模拟核心账务：余额扣减、故障/超时注入、错误归一化
internal/gateway/       请求判定流水线（串联以上组件）
internal/httpapi/       JSON over HTTP 适配层
```

## 请求判定流水线（写入类）

```
validate → 分钟窗口限流 → 同意鉴权 → 幂等 Begin ── 新请求 → 日配额 → 核心账务 → 幂等 Finish
                                         └── 回放 → 返回首次稳定结果（成功或失败）
                                         └── 冲突 → IDEMPOTENCY_CONFLICT
```

每个环节拒绝时短路返回，并打印一条结构化决策日志（见文末"日志与可解释性"）。

## 1. 同意与授权模型

同意对象在建立时绑定五元组：

| 字段 | 含义 |
|---|---|
| `TenantID` | 租户归属（金融机构/牌照主体） |
| `ClientID` | 发起请求的 TPP 客户端 |
| `AccountID` | 授权指向的单一账户 |
| `Scopes` | 授权范围：`accounts.read` / `payments.initiate` |
| `ExpiresAt` | 到期时刻（建立时刻 + TTL） |

同意状态：`active` →（`revoked` | `expired`），终态不可逆。

### 鉴权判定顺序与可区分的拒绝原因

`consent.Service.Check` 按固定顺序判定，拒绝原因**互不相同、稳定可机读**：

| 顺序 | 判定 | 错误码 | HTTP |
|---|---|---|---|
| 1 | 同意不存在 | `CONSENT_MISSING` | 404 |
| 2 | 请求租户 ≠ 同意租户 | `CROSS_TENANT_ACCESS` | 403 |
| 3 | 租户相同但客户端不同 | `CLIENT_MISMATCH` | 403 |
| 4 | 请求账户 ≠ 同意绑定账户 | `ACCOUNT_MISMATCH` | 403 |
| 5 | 已撤销（`status=revoked`） | `CONSENT_REVOKED` | 403 |
| 6 | 已过期（`now >= ExpiresAt`，或 `status=expired`） | `CONSENT_EXPIRED` | 403 |
| 7 | 同意不携带所需 scope | `SCOPE_INSUFFICIENT` | 403 |

要点：

- **到期边界精确**：`now == ExpiresAt` 即判为过期；前 1 纳秒仍放行（有单测锁定）。
- 到期除了判定时惰性生效外，`SweepExpired` 可显式把过期同意落库为 `expired`，便于审计。
- 撤销是幂等操作，重复撤销时间戳不变。
- **同意鉴权先于幂等回放**：即使幂等键此前成功过，只要本次同意已撤销/过期/越权，
  依然被拒绝，不会用旧授权回放旧结果。
- 读取接口 `GET /accounts/{id}` 复用同一套判定，所需 scope 为 `accounts.read`。

## 2. 幂等语义

- 每个写入请求必须携带 `Idempotency-Key`（JSON 字段或同名 HTTP 头）。
- 幂等记录的作用域是 **(键, 租户)**：相同键字符串被另一租户复用即视为冲突，
  绝不跨租户回放。
- 请求**业务内容**（租户/客户端/同意/账户/金额/参考号，**不含幂等键本身**）被
  确定性序列化为 SHA-256 指纹。
  - **键相同 + 内容相同**：首个调用执行并落结果；并发的重复调用阻塞等待首个调用
    完成，随后拿到**同一个稳定结果**（`replayed=true`，含相同 `payment_id`），
    核心账务**只入账一次**。
  - **键相同 + 内容不同**：立即返回 `IDEMPOTENCY_CONFLICT`（409），**不会**静默按
    首次请求回放；已存储的首次结果不受影响，仍可被同内容请求回放。
- 首次调用若被核心账务拒绝（如余额不足、上游故障/超时），该**失败结果同样被记录
  并在重试时稳定回放**，错误码一致，避免"重试时偶发成功"造成的不确定性。
- 若首次调用在真正触达账务前就被拒绝（如日配额耗尽），in-flight 占位通过
  `FailInflight` 释放：本次不入账、不毒化键，窗口恢复后同键重试可以正常执行。
- 并发与取消安全基于互斥锁 + 每个键一个 `done` channel；等待者自己的 `context`
  超时不会破坏已存在的记录（有竞态检测测试覆盖）。

## 3. 限流与配额规则（窗口确定且可复现）

限流维度是 **(租户, 客户端)**，每个主体拥有完全独立的计数器：

- **分钟窗口（RatePerMinute）**：固定窗口，窗口键为 `floor(unix时间戳 / 60)`。
  同一分钟内累计计数，跨整分钟边界时计数清零——**不是滑动窗口**，边界与计数均可
  精确复现（测试中用可注入时钟锁定）。
  - 所有到达请求（含最终被同意层拒绝、含幂等回放）都计入该窗口，因为对外部观察者
    而言它们都是流量。
  - 超过窗口额度返回 `RATE_LIMITED`（429）。
- **日配额（QuotaPerDay）**：窗口键为 UTC 日历日 `YYYY-MM-DD`，在 UTC 00:00 清零。
  - 仅对**真正的新写入**计数；幂等回放不消耗配额。
  - 超额返回 `QUOTA_EXCEEDED`（429），且发生在核心账务调用之前，**不会入账**。
- 判定"先决策后扣减"：被拒绝的请求不留计数痕迹。
- **租户隔离**：某 (租户, 客户端) 被限流或被上游拒绝，绝不影响任何其他主体的窗口
  与配额；可通过 `GET /debug/counters` 查看原始计数。
- 未配置规则的主体默认放行，方便本地实验。

### 上游故障归一化

核心账务的任何异常都不会把底层错误（含 `context deadline exceeded` 等）直接透出，
而是映射为稳定类别：

| 场景 | 归一化错误码 | HTTP |
|---|---|---|
| 上游在 deadline 内未响应（注入延迟 > 请求超时） | `UPSTREAM_TIMEOUT` | 504 |
| 上游显式不可用/未分类故障 | `UPSTREAM_UNAVAILABLE` | 502 |
| 上游业务拒绝（未知账户、余额不足、币种非法） | `UPSTREAM_REJECTED` | 502 |

模拟账务可按支付参考号注入故障与延迟（`ledger.InjectFault`），用于确定性复现。

## 4. 本地运行

要求 Go 1.25+。

```bash
go run ./cmd/gateway -addr :8099
# 预置账户：acct-alice 余额 100000；acct-bob 余额 5000
# 预置规则：tenant-a/client-1 = 30 次/分钟, 1000 笔/日
#           tenant-b/client-1 =  5 次/分钟,  100 笔/日
```

### curl 全流程演练

```bash
B=http://127.0.0.1:8099

# 1) 建立同意（2 小时有效，支付范围）
CID=$(curl -s -X POST $B/consents -d '{
  "tenantId":"tenant-a","clientId":"client-1","accountId":"acct-alice",
  "scopes":["payments.initiate"],"ttlHours":2}' | tee /dev/stderr | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')

# 2) 发起支付 -> 201
curl -i -X POST $B/payments -H 'Content-Type: application/json' -d "{
  \"idempotencyKey\":\"key-1\",\"tenantId\":\"tenant-a\",\"clientId\":\"client-1\",
  \"consentId\":\"$CID\",\"accountId\":\"acct-alice\",
  \"amount\":{\"amount\":1500,\"currency\":\"EUR\"},\"reference\":\"ref-1\"}"

# 3) 完全相同的重试 -> 200, replayed=true, payment_id 与首次一致（不重复入账）
curl -i -X POST $B/payments -H 'Content-Type: application/json' -d "{
  \"idempotencyKey\":\"key-1\",\"tenantId\":\"tenant-a\",\"clientId\":\"client-1\",
  \"consentId\":\"$CID\",\"accountId\":\"acct-alice\",
  \"amount\":{\"amount\":1500,\"currency\":\"EUR\"},\"reference\":\"ref-1\"}"

# 4) 同键但金额被篡改 -> 409 IDEMPOTENCY_CONFLICT
curl -i -X POST $B/payments -H 'Content-Type: application/json' -d "{
  \"idempotencyKey\":\"key-1\",\"tenantId\":\"tenant-a\",\"clientId\":\"client-1\",
  \"consentId\":\"$CID\",\"accountId\":\"acct-alice\",
  \"amount\":{\"amount\":9999,\"currency\":\"EUR\"},\"reference\":\"ref-1\"}"

# 5) 跨租户使用 -> 403 CROSS_TENANT_ACCESS
curl -i -X POST $B/payments -H 'Content-Type: application/json' -d "{
  \"idempotencyKey\":\"x1\",\"tenantId\":\"tenant-b\",\"clientId\":\"client-1\",
  \"consentId\":\"$CID\",\"accountId\":\"acct-alice\",
  \"amount\":{\"amount\":10,\"currency\":\"EUR\"},\"reference\":\"x\"}"

# 6) 撤销后再支付 -> 403 CONSENT_REVOKED
curl -s -X POST $B/consents/$CID/revoke >/dev/null
curl -i -X POST $B/payments -H 'Content-Type: application/json' -d "{
  \"idempotencyKey\":\"rv\",\"tenantId\":\"tenant-a\",\"clientId\":\"client-1\",
  \"consentId\":\"$CID\",\"accountId\":\"acct-alice\",
  \"amount\":{\"amount\":10,\"currency\":\"EUR\"},\"reference\":\"rv\"}"
```

快速验证限流与租户隔离（tenant-b 限额 5 次/分钟）：

```bash
CBID=$(curl -s -X POST $B/consents -d '{"tenantId":"tenant-b","clientId":"client-1",
  "accountId":"acct-alice","scopes":["payments.initiate"],"ttlHours":2}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
for i in $(seq 1 6); do
  curl -s -o /dev/null -w "%{http_code} " -X POST $B/payments -d "{
    \"idempotencyKey\":\"bk$i\",\"tenantId\":\"tenant-b\",\"clientId\":\"client-1\",
    \"consentId\":\"$CBID\",\"accountId\":\"acct-alice\",
    \"amount\":{\"amount\":1,\"currency\":\"EUR\"},\"reference\":\"b$i\"}"
done; echo   # 前 5 个成功，第 6 个 429；tenant-a 不受影响
```

### HTTP API 一览

| 方法与路径 | 说明 |
|---|---|
| `POST /consents` | 建立同意（body 见上；`ttlHours` 缺省 1） |
| `POST /consents/{id}/revoke` | 撤销同意（幂等） |
| `POST /payments` | 发起支付；需 `idempotencyKey`（字段或头） |
| `GET /accounts/{id}?tenantId&clientId&consentId` | 读取账户摘要（需 `accounts.read`） |
| `GET /debug/counters?tenantId&clientId` | 查看分钟/日窗口原始计数 |
| `GET /healthz` | 健康检查 |

## 5. 测试

```bash
# 全量单测（含 -race 竞态检测）
go test ./... -race -count=1

# 只看同意边界
go test ./internal/consent/ -v
# 只看并发重复提交（40 个 goroutine 只允许入账一次，其余全部回放同一 payment_id）
go test ./internal/gateway/ -race -v -run TestConcurrentDuplicatesPostExactlyOnceEndToEnd
go test ./internal/idempotency/ -race -v -run TestConcurrentDuplicatesPostExactlyOnce
```

测试覆盖的重点（与需求对应）：

- **同意边界**：缺省同意、跨租户、跨客户端、账户不符、范围不足、撤销、过期
  （含到期边界精确到纳秒）全部以**不同错误码**拒绝。
- **并发重复提交**：32/40 个并发同键请求仅 1 次真实执行，其余全部回放同一
  `payment_id`，核心账务行数恒为 1；上游失败也被稳定回放为同一错误码。
- **幂等键冲突**：同键不同内容明确 409，且不污染首次结果。
- **窗口与隔离**：固定分钟边界重置、UTC 日界重置、读不计写配额、租户间互不影响。
- 单测中每个用例都通过 `t.Logf` 打印**请求关键字段**（租户/客户端/同意/账户/
  scope/金额/幂等键）与**判定依据**（同意归属、状态、到期时刻、当前时钟、窗口
  计数等），`go test -v` 即可看到。

## 6. 日志与可解释性

每次判定输出一行决策日志，例如：

```
DECIDE reject key=k-x1 tenant=tenant-b client=client-1 consent=cons-... account=acct-alice
  ref=r-x stage=consent code=CROSS_TENANT_ACCESS
  detail="consent tenant \"tenant-a\" does not match request tenant \"tenant-b\""
DECIDE replay key=key-1 paymentID=pay-... stable result returned (no new ledger write)
```

`stage` 标识在流水线的哪个环节作出结论（validate / ratelimit / consent /
idempotency / quota / ledger / replay），`code` 是稳定错误码，`detail` 给出人类可读
依据——跨租户、过期、撤销、范围不足、键冲突等结论因此始终可区分、可解释。
