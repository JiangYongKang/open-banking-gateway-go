# Open Banking Gateway（本地模拟版）

本地可运行的开放银行接口网关与同意授权流程实现：基于本地内存存储与模拟核心账务服务，
验证同意边界、幂等并发安全、按租户/客户端限流以及错误归一化。

## 目录结构

```
internal/
  consent/      同意生命周期（建立/撤销/到期）与授权判定
  idempotency/  幂等键状态机（占用/回放/冲突/放弃）
  ratelimit/    按 (租户, 客户端) 维度的固定窗口限流
  quota/        同意级周期用量额度（笔数/累计金额，预占-实扣）
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
   **不会二次入账**。
3. **同键但内容不一致**：返回 `IDEMPOTENCY_CONFLICT`，**拒绝而不是静默回放**。
4. **同键并发处理中**：同指纹返回“in-flight”冲突，不同指纹返回内容冲突。
5. **账务写入失败**：幂等占用被 `Abandon`，故障恢复后允许同键重试；
   写入成功则结果永久保存，后续重试一律回放。

并发安全由互斥锁 + 单写者占用保证（`-race` 下 32 个并发重复提交仅产生 1 条流水）。

## 三、限流窗口规则

- 维度：`(TenantID, ClientID)` 独立计数，**租户/客户端之间互不影响**；
  一个租户被限流不影响任何其他租户。
- 算法：固定窗口。窗口起点用 `now.Truncate(window)` 对齐到固定边界
  （如 1 分钟窗口对齐整分钟），因此**窗口重置点与重置时刻确定、可复现**，
  不依赖首个请求的到达时间。
- 每次放行消耗 1 个配额；拒绝响应返回剩余配额与 `ResetAt`（RFC3339）。
- 被限流的请求不进入幂等与账务阶段。

## 四、同意级周期额度

建立同意时可选配 `quota.Limit`（`Consent.Quota`，为 `nil` 表示不配额度，行为与之前完全一致）：

```go
Quota: &quota.Limit{Period: quota.PeriodWeekly, MaxCount: 100, MaxAmount: 1_000_000}
```

- **周期模型**：`DAILY`（自然日，00:00 UTC 对齐）、`WEEKLY`（自然周，周一 00:00 UTC
  对齐，ISO 8601）、`MONTHLY`（自然月，1 日 00:00 UTC 对齐）。窗口起点由当前时刻
  向下对齐到固定边界，**与请求到达时间无关，重置点确定、可复现**；时间源可注入，
  测试中可确定性推进跨周期。
- **上限维度**：`MaxCount`（每周期最大笔数）与 `MaxAmount`（每周期累计金额，最小货币
  单位）可单独或同时配置；某维度为 0 表示该维度不限（查询时对应 `Remaining` 为 -1）。
- **扣减时机**：请求通过同意校验、限流、幂等占用后做**预占**（tentative 计入已用），
  账务写入**最终成功**才 `Commit` 落定为实扣；上游拒绝/超时/内部错误时 `Release`
  全额退回，**失败不扣额度**，故障恢复后同键重试可正常扣减。
- **恰好一次**：预占按 `(同意, 周期窗口, 幂等键)` 去重——同一幂等键无论重复提交还是
  并发重试风暴，全周期只扣一次；幂等回放直接返回首个结果，不触碰额度。
- **隔离**：用量按同意 ID 完全隔离，一个同意用尽不影响其他同意，也不影响未配额度
  的同意。
- **额度用尽的拒绝**：返回独立错误码 `QUOTA_EXCEEDED`（与 `CONSENT_DENIED`、
  `RATE_LIMITED` 明确区分），消息携带本周期 `remaining_count` / `remaining_amount`
  与 `reset_at`（RFC3339）。被挡下的写入会释放幂等占用，下一周期同键重试可正常执行。
- **用量查询**：`Gateway.QuotaStatus(tenantID, consentID)` 返回当前周期已用、剩余与
  下次重置时间；未配额度的同意返回 `(nil, nil)`；跨租户查询按 `CONSENT_DENIED` 拒绝。

## 五、错误归一化

底层异常不会透出，网关只返回可区分的错误类别（`gateway.Code`）：

| 网关错误码 | 含义 |
|---|---|
| `CONSENT_DENIED` | 同意校验未通过（附具体拒绝原因） |
| `RATE_LIMITED` | 触发限流配额 |
| `QUOTA_EXCEEDED` | 同意本周期额度用尽（附剩余量与重置时间） |
| `IDEMPOTENCY_CONFLICT` | 幂等键内容冲突或同键处理中 |
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

# 带判定日志运行（打印请求关键字段与判定依据）
go test ./... -run . -v

# 运行本地演示：首次写入、幂等回放、键冲突、缺省/过期/撤销/越权同意、限流、周期额度
go run ./cmd/gateway

# 只跑额度相关用例（含并发只扣一次、用尽可区分、跨周重置、失败不扣、同意间隔离）
go test ./internal/quota/ ./internal/gateway/ -run 'Quota|WeeklyReset' -race -count=1 -v
```

演示程序输出示例：

```
[first write           ] OK entry=txn-1 replayed=false
[idempotent replay     ] OK entry=txn-1 replayed=true
[key reuse conflict    ] REJECTED IDEMPOTENCY_CONFLICT: ...
[missing consent       ] REJECTED CONSENT_DENIED: CONSENT_MISSING
[cross-tenant          ] REJECTED CONSENT_DENIED: CONSENT_TENANT_MISMATCH
[expired consent       ] REJECTED CONSENT_DENIED: CONSENT_EXPIRED
[scope insufficient    ] REJECTED CONSENT_DENIED: CONSENT_SCOPE_INSUFFICIENT
[revoked consent       ] REJECTED CONSENT_DENIED: CONSENT_REVOKED
[weekly quota #1       ] OK entry=txn-2 replayed=false
[weekly quota #2       ] OK entry=txn-3 replayed=false
[weekly quota #3       ] REJECTED QUOTA_EXCEEDED: consent period allowance exhausted, remaining_count=0 remaining_amount=-1 reset_at=2026-10-12T00:00:00Z
[quota status          ] used=2 remaining=0 reset_at=2026-10-12T00:00:00Z
```
