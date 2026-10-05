# Open Banking Gateway（本地模拟版）

本地可运行的开放银行接口网关与同意授权流程实现：基于本地内存存储与模拟核心账务服务，
验证同意边界、同意级周期用量额度、幂等并发安全、按租户/客户端限流以及错误归一化。

## 目录结构

```
internal/
  consent/      同意生命周期（建立/撤销/到期）与授权判定，可携带可选额度策略
  quota/        同意级周期性用量额度（笔数/累计金额上限，三段式扣减）
  idempotency/  幂等键状态机（占用/等待结算/回放/冲突/放弃）
  ratelimit/    按 (租户, 客户端) 维度的固定窗口限流
  ledger/       模拟核心账务服务（可注入超时/拒绝/内部故障）
  gateway/      编排：同意 -> 限流 -> 幂等 -> 额度 -> 账务写入，归一化错误
cmd/gateway/    本地演示程序
```

## 一、同意与授权模型

`Consent` 归属唯一租户（`TenantID`），记录客户端、授权范围（`Scopes`）、有效期
（`ExpiresAt`）与状态（`ACTIVE` / `REVOKED`）。

写入请求必须携带 `ConsentID` 与所需范围。授权判定按**固定顺序**执行，保证拒绝原因
确定、可区分、可解释：

| 顺序 | 判定 | 拒绝原因（`consent.Reason`） | 网关错误码 |
|---|---|---|---|
| 1 | 同意是否存在 | `CONSENT_MISSING` | `CONSENT_DENIED` |
| 2 | 租户是否归属 | `CONSENT_TENANT_MISMATCH` | `CONSENT_DENIED` |
| 3 | 是否已撤销 | `CONSENT_REVOKED` | `CONSENT_DENIED` |
| 4 | 是否到期（`now >= ExpiresAt`） | `CONSENT_EXPIRED` | `CONSENT_DENIED` |
| 5 | 请求所需范围是否全部被覆盖 | `CONSENT_SCOPE_INSUFFICIENT` | `CONSENT_DENIED` |

- 同意一经撤销不可恢复，撤销即时生效（读时判定，不依赖定时任务）。
- 跨租户请求只能看到“归属不匹配”，不会泄露其他租户的同意详情。
- 时间源可注入（`func() time.Time`），到期边界可在测试中确定性验证。

## 二、幂等语义

客户端通过 `IdempotencyKey` 标识一次写入，网关对请求内容做 SHA-256 指纹
（租户、客户端、同意、范围、账户、金额、币种、附言均参与）。

幂等键状态机（`internal/idempotency`）：

1. **首次提交**：键被标记为 `in-flight`，账务写入成功后记录结果（200 + 响应体）。
2. **重复提交（同键、同指纹）**：直接回放首次结果（`Replayed=true`），
   **不会二次入账、不会二次扣额度**。
3. **同键但内容不一致**：返回 `IDEMPOTENCY_CONFLICT`，**拒绝而不是静默回放**。
4. **同键并发处理中**：网关使用 `BeginOrWait` 阻塞等待首个请求落定——成功则
   **回放同一笔结果**，首个请求失败放弃后则竞争成为新的处理者（重试风暴收敛）；
   底层存储的 `Begin` 仍保留“立即返回 in-flight 冲突”的非阻塞语义。
5. **账务写入失败**：幂等占用被 `Abandon`、额度预占被 `Release`，故障恢复后
   允许同键重试；写入成功则结果永久保存，后续重试一律回放。

并发安全由互斥锁 + 单写者占用保证（`-race` 下 32 个并发重复提交仅产生 1 条流水）。

## 三、限流窗口规则

- 维度：`(TenantID, ClientID)` 独立计数，**租户/客户端之间互不影响**；
  一个租户被限流不影响任何其他租户。
- 算法：固定窗口。窗口起点用 `now.Truncate(window)` 对齐到固定边界
  （如 1 分钟窗口对齐整分钟），因此**窗口重置点与重置时刻确定、可复现**，
  不依赖首个请求的到达时间。
- 每次放行消耗 1 个配额；拒绝响应返回剩余配额与 `ResetAt`（RFC3339）。
- 被限流的请求不进入幂等、额度与账务阶段。

## 四、同意级周期用量额度

每条同意在建立时可**可选**携带一份周期额度策略（`consent.Consent.Quota` / `quota.Policy`）：

| 字段 | 含义 |
|---|---|
| `Period` | 周期类别：`WEEKLY`（自然周）或 `DAILY`（自然日） |
| `MaxCount` | 每周期最多**成功**写入笔数；`<=0` 表示不限笔数 |
| `MaxAmount` | 每周期成功写入的累计金额上限；`<=0` 表示不限金额 |
| `Currency` | 金额上限币种；配置了 `MaxAmount` 时必填 |

笔数与金额两个维度可单独配置，也可同时配置；判定顺序固定为
**币种 → 笔数 → 金额**，拒绝原因分别为 `QUOTA_CURRENCY_MISMATCH`、
`QUOTA_COUNT_EXCEEDED`、`QUOTA_AMOUNT_BUDGET_EXCEEDED`。

**未配置额度的同意行为与以前完全一致**，不会经过任何额度判定。

### 周期与重置模型（固定、可复现）

- 所有边界按 **UTC 自然时间**对齐，与首个请求到达时刻无关：
  - 自然日 = UTC `00:00` 到次日 `00:00`；
  - 自然周 = **周一 00:00 UTC** 到下周一 `00:00 UTC`（`quota.periodBounds`）。
- 时间源可注入（`quota.NewManager(nowFunc)`），跨周期重置可在测试中确定性验证；
  到达新周期后已用量自动归零，新写入按新窗口计数。
- 可随时通过 `Manager.GetUsage(consentID)` 查询当前周期的
  `PeriodStart` / `ResetAt` / `UsedCount` / `UsedAmount` 与剩余量
  （`RemainingCount()` / `RemainingAmount()`，未配置维度返回 `-1=unlimited`）。

### 扣减时机：与写入最终结果严格一致

额度采用 **Hold（预占）→ Commit（提交）/ Release（释放）** 三段式，
只在账务写入**真正成功**后扣减：

1. 请求依次通过同意、限流、幂等三关后调用 `Hold` 预占本次笔数与金额；
   预占即计入“已用”，因此并发请求不会把同一笔预算超卖到两个人。
2. 账务写入**成功**：`Commit` 落地，额度真正扣减，再固化幂等结果。
3. 上游拒绝、上游超时或任何内部错误：`Release` **原样归还**预占，额度不扣减；
   同时幂等占用被 `Abandon`，故障恢复后同键重试可正常扣减。
4. 被额度挡下的请求不会入账，幂等占用同步释放（下一周期/补额后同键可重试）。

**同一幂等键只扣一次**：`Hold` 以幂等键去重，串行重复提交与并发重试
（无论处理中还是已提交）都不会二次扣减；即使跨周期，上一周期已提交的幂等键
在新窗口重现也只回放、不再扣减。

**重试风暴收敛**：幂等存储新增 `BeginOrWait`——同键同指纹在首个请求处理中时
阻塞等待其落定：成功则所有等待方**回放同一笔结果**（相同 entry ID，
`Replayed=true`），首个请求失败放弃后等待方之一成为新的处理者继续重试。
因此客户端超时后立刻重发，各方最终要么都得到同一笔成功、要么共同可见同一失败，
不会出现有人成功有人失败，也不会双扣额度。

**隔离性**：额度账本以同意 ID 为键独立维护（`quota.Manager` 内部
`map[consentID]*state`，互斥保护），不同租户、不同同意之间完全隔离；
一条同意用尽不影响任何其他同意。

### 与现有三关的关系

请求必须同时通过三道关卡才放行，顺序固定、拒绝原因互不混淆：

```
同意有效性 (CONSENT_DENIED) → 限流 (RATE_LIMITED) → 幂等占用/等待 → 同意额度 (QUOTA_EXHAUSTED) → 账务写入
```

- 额度拒绝是**独立错误码 `QUOTA_EXHAUSTED`**，与同意过期/撤销/范围不足、
  限流、幂等冲突都可直接区分；错误体还携带 `QuotaReason` 与 `Usage`
  （本周期剩余笔数/金额、下次重置时间）。
- 幂等回放发生在额度判定之前：回放请求不重复扣额度，响应中仍附当前周期用量。

## 五、错误归一化

底层异常不会透出，网关只返回可区分的错误类别（`gateway.Code`）：

| 网关错误码 | 含义 |
|---|---|
| `CONSENT_DENIED` | 同意校验未通过（附具体拒绝原因） |
| `RATE_LIMITED` | 触发 (租户, 客户端) 限流配额 |
| `QUOTA_EXHAUSTED` | 同意级周期额度用尽（附笔数/金额原因、剩余量与重置时间） |
| `IDEMPOTENCY_CONFLICT` | 幂等键内容冲突（处理中同键改为等待收敛，不再报此错） |
| `UPSTREAM_REJECTED` | 上游核心账务明确拒绝 |
| `UPSTREAM_TIMEOUT` | 上游核心账务超时 |
| `INTERNAL` | 内部故障（未分类的底层异常统一归一） |

## 六、本地验证

要求 Go 1.25+。

```bash
# 编译
go build ./...

# 运行全部单测（含竞态检测）
go test ./... -race -count=1

# 只跑额度相关用例（quota 包 + gateway 集成）
go test ./internal/quota/ ./internal/gateway/ -race -count=1 -v

# 带判定日志运行（打印每类场景的输入与结论）
go test ./... -run . -v

# 运行本地演示：首次写入、幂等回放、键冲突、缺省/过期/撤销/越权同意、
# 周期额度用尽与回放、金额预算、限流
go run ./cmd/gateway
```

额度相关单测覆盖：

- `quota.TestDuplicateAndConcurrentKeyDeductOnce`：串行重复 + 32 并发同键只扣一次；
- `gateway.TestQuotaReplayDeductsOnceAndStormConverges`：32 路重试风暴收敛到同一笔
  （1 次写入 + 31 次回放），新键被额度挡下；
- `gateway.TestQuotaCountExhaustionDistinctCode` / `TestQuotaAmountExhaustion`：
  笔数/金额用尽被拒、错误码与原因可区分、返回剩余与重置时间；
- `gateway.TestQuotaWeeklyRollover` / `quota.TestWeeklyRolloverResets`：跨自然周重置，
  旧周期已提交幂等键不二次扣减；
- `gateway.TestQuotaUpstreamFailureDoesNotConsume` / `quota.TestReleaseOnFailureRestoresBudget`：
  上游超时/失败不扣额度，恢复后同键重试成功且只扣一次；
- `quota.TestIsolationBetweenConsents` / `gateway.TestQuotaIsolationBetweenConsents`：
  不同同意、无额度同意之间互不影响。

演示程序输出示例（节选）：

```
[quota write #1        ] OK entry=txn-2 replayed=false
[quota write #2        ] OK entry=txn-3 replayed=false
[quota write #3 (exhaust)] REJECTED QUOTA_EXHAUSTED: consent quota exhausted (QUOTA_COUNT_EXCEEDED), remaining count=0 amount=unlimited, resets at 2026-10-12T00:00:00Z
[quota idempotent replay] OK entry=txn-2 replayed=true
[amount quota write #1 ] OK entry=txn-4 replayed=false
[amount quota write #2 (over)] REJECTED QUOTA_EXHAUSTED: consent quota exhausted (QUOTA_AMOUNT_BUDGET_EXCEEDED), remaining count=unlimited amount=500 CNY, resets at 2026-10-12T00:00:00Z
[rate limit #6         ] REJECTED RATE_LIMITED: quota exhausted, retry after ...
```
