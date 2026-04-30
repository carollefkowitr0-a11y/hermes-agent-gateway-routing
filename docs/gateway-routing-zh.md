# Hermes Gateway 多 Bot / 多 Profile 路由方案思路

## 1. 背景

这次改动围绕 Hermes Gateway 的 Telegram 接入层展开，目标不是立刻把公网 webhook 暴露为生产入口，而是在安全边界内搭出一套可验证、可回滚、可逐步放大的多 Bot / 多 Profile 路由框架。

核心场景是：不同专家 profile 可以拥有自己的 Telegram Bot；明确归属某个专家的消息直接进入该专家的队列；未归属、歧义或需要调度的消息进入默认总管，由总管再分配给专家团队。

## 2. 设计原则

1. **先本地闭环，再接生产流量**
   所有关键链路先通过本地 SQLite spool、dry-run worker、注入式 Telegram client 和测试用 WSGI ingress 验证，不默认触发真实 Telegram API、真实唤醒或外部进程控制。

2. **默认安全，显式越权**
   高风险动作如唤醒、启动、停止、重启 profile/worker 默认只生成计划，不直接执行；需要非 dry-run 或公网绑定时必须显式开启，并由调用方承担审批语义。

3. **隐私最小暴露**
   对外摘要只返回计数、状态、路由标签、row id 等必要元数据；不回显消息正文、chat_id、user_id、thread_id、token、env、argv 或 payload_json。

4. **队列先行，处理解耦**
   ingress 只负责认证、校验、入队；worker/controller 再从 spool 中取任务处理。这样可以抵抗短暂失败、支持重复去重，也方便审计。

5. **适配当前基础设施约束**
   在只有国内 VPS、Telegram 访问受限的前提下，方案优先支持 outbound polling / long-poll watcher，而不是依赖公网 webhook 或临时隧道。

## 3. 主要模块

### 3.1 SQLite Spool

`gateway/spool.py` 提供本地持久队列：

- 使用 SQLite + WAL；
- 按 `platform + route + update_id` 去重；
- 支持 `queued / dispatched / failed / dead_letter` 等状态；
- 数据库和 WAL 文件尽量限制为 `0600` 权限；
- worker 可以按 `target_profile` 拉取最早的 queued 事件。

### 3.2 Shadow Ingress 与本地 HTTP Ingress

`gateway/shadow_ingress.py` 和 `gateway/http_ingress.py` 提供安全的影子 webhook 路径：

- `/healthz` 检查进程可达；
- `/readyz` 检查 spool 可用；
- `/telegram/naval/shadow` 接收测试 Telegram payload；
- 校验 content-type、body 大小、JSON 格式和测试 secret；
- 入队结果不回显敏感字段。

`gateway/ingress_runner.py` 是本地 WSGI runner，默认只绑定 `127.0.0.1`，拒绝公网 bind，除非显式传入 `--allow-public-bind`。

### 3.3 多 Profile Polling Watcher

`gateway/polling_watcher.py` 搭出多 Bot / 多 Profile 的 long-poll 框架：

- 每个 profile 必须拥有唯一 token 引用；
- 支持启用/禁用 profile；
- 支持 owned profile 直达；
- 支持 unowned / ambiguous 消息回落到 manager profile；
- offset 原子写入，避免重复消费；
- 默认通过注入 client 进行测试，不默认访问真实 Telegram。

### 3.4 Wake / Dispatch 安全边界

`gateway/commands.py`、`gateway/dispatcher.py`、`gateway/wake_controller.py`、`gateway/supervisor_boundary.py`、`gateway/spool_worker.py` 共同定义处理边界：

- `GatewayCommand` 只携带安全元数据；
- 高风险动作必须 dry-run 且 requires_approval；
- `dispatch_one` 每次最多处理一条 queued row；
- `WakeController` 负责从计划到调度的一次迭代；
- `spool_worker` 默认只处理 naval profile，非 naval 默认拒绝，防止误触发其他 profile。

### 3.5 Webhook Helper

`gateway/webhook_helper.py` 提供 Telegram webhook 操作的安全包装：

- 默认 dry-run；
- 只有显式传入 client 时才调用 Telegram API；
- 对 token、secret、URL 查询参数、错误信息等进行脱敏；
- 支持 capture / set / delete / restore webhook 的计划化输出。

## 4. 路由模型

当前路由模型可以概括为三层：

1. **专家 Bot 直达**
   如果消息来自某个明确归属的专家 bot，例如 naval bot，则进入该专家 profile 的 spool。

2. **默认总管兜底**
   如果消息来源未归属、歧义、或配置为 manager fallback，则进入 default profile，由总管做二次判断和调度。

3. **worker 逐步放权**
   当前 worker 默认只开放 naval 的安全路径；其他 profile 需要显式扩展和测试，避免一开始把所有专家入口都接入真实执行链路。

## 5. 验证策略

这次改动配套了单元测试，覆盖：

- spool 初始化、入队、去重、权限、状态流转；
- shadow ingress 的合法/非法 payload；
- HTTP ingress 的认证、content-type、body size、JSON 校验；
- 多 profile polling 的 token 唯一性、offset、fallback、dry-run wake；
- worker/controller/dispatcher 的一次性处理与失败语义；
- webhook helper 的脱敏和 dry-run 行为；
- evidence 输出不泄漏私密字段。

## 6. 后续推进建议

1. **先在本地/影子链路持续跑通**：使用测试 payload 和 dry-run worker 验证端到端状态变化。
2. **再接入 outbound polling**：在 VPS 上用代理访问 Telegram getUpdates，不要求 Telegram 访问 VPS 公网 IP。
3. **最后再评估 webhook**：只有在有稳定公网入口和明确安全策略时，才把 webhook 从 shadow 模式提升到生产模式。
4. **逐个专家 profile 放开**：不要一次性接入所有专家 bot；每个 profile 都应有独立 spool、测试、回滚路径和安全门。

## 7. 一句话总结

这套改动的本质是：先把 Hermes Gateway 从“单入口消息转发”推进到“多 Bot、多 Profile、队列化、可审计、默认安全”的路由底座，同时严格避免在未验证阶段触发真实生产副作用。
