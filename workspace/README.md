# 药物不良事件病例接收与监管报送服务（Pharmacovigilance Case & Reporting Service）

接收医院、患者热线、合作企业三渠道的不良事件报文，处理重复上报、随访更正、
监管加急报送与报告版本追溯。**纯 Python 标准库实现**（Python 3.11+，SQLite），
无任何第三方依赖。

## 需求映射

| 需求 | 实现 |
|---|---|
| 字段缺失 / 姓名别名 / 多语言导致重复上报 | `normalize.py`：中/英/西语药名、反应、严重性、因果性、姓名别名词典；宽松日期解析 |
| 给出**可复核**疑似重复簇，专员确认合并 | `dedup.py` 加权打分（强标识 100 分短路；姓名+DOB+药+反应+日期组合 ≥70），每条边带分值与命中理由；`PROPOSED → CONFIRMED/REJECTED` 只有人工能驱动 |
| 原始来源记录始终保留 | `source_records`、`case_versions`、`case_events` 全部只增，由 SQLite 触发器拒绝 UPDATE/DELETE |
| 随访修正用药时间/严重性/因果性 | 白名单补丁 + `new_reactions/new_drugs/drug_start_dates`，产生**新版本**而非覆盖 |
| 按规则判断加急 | `rules.py`：严重×非预期=15 日加急；严重已预期 30 日；非严重 90 日；无实质新信息不重复报送。每条决策带 `rule_id`+理由 |
| 新版本更正既有报告 | FOLLOWUP 报告 `followup_of_id` 关联原报告，payload 携带 `supersedes_regulatory_numbers` |
| 来源任何时刻只归属一个当前病例 | `case_membership` 只增（旧行 1→0 停用+新行插入）+ 部分唯一索引 `WHERE active=1` 数据库兜底 |
| 合并/拆分与回执并发 | 进程内共享 RLock + `BEGIN IMMEDIATE`；转移前再校验 active 归属，冲突返回 409 |
| 已发送报告可追溯 | 报告行永久保留；病例血缘闭包 `case_lineage` 使合并/拆分后的完整时间线可从存活病例追溯 |
| 重试/重启不产生重复监管编号 | 编号在业务事务内由序列表分配 + `regulatory_number UNIQUE`；发送只是状态机（QUEUED→SENDING→ACKED/FAILED/VOID）；网关以编号为幂等键；重启把 SENDING 重置回 QUEUED，重放取回同一 ack |
| 病例工作台 / 规则化接口 / 版本时间线 | HTTP JSON API（见下） |
| **药物安全决定双人会签** | `countersign.py` + `revision.py`：不可变评审单（候选归并/严重等级变化/待发送决定冻结，触发器禁改）；两名不同复核员对同一数据修订号签署才放行，同人重复提交不累计，任一人否决即终态；**复核员身份按 `identity.py` NFKC+大小写+首尾空白规范化稳定判定，空白/非法身份明确拒绝，原始提交值原样留痕，旧库启动时迁移到规范化身份键**；新材料/归并/拆解/规则重算在业务事务内推进修订号并原子作废旧单（意见保留但不能发送）；租约原子领取/到期释放/重启恢复；放行按 `report_id` 只解锁一次；未会签强制拦截发送，老调用可用显式兼容开关 |
| 可移交的证据包导出与离线核验 | `export_service.py`：自包含 tar（来源原文/版本快照/合并拆分关系/报送回执/审计时间线），清单根 + 每文件 SHA-256 + Ed25519 签名；`verifier.py` 在另一进程纯离线逐项核验 |
| **药物安全法规规则变更：影响分析与受控启用** | `rule_pkg.py`（不可变规则包内容/HMAC 签名/生效区间/版本化引擎）+ `rule_gov.py`（登记三校验、历史快照试算、受控启用/回滚/定时、检查点续跑、已发送只生成更正候选、治理审计哈希链）；计算/会签/发送路径固定本次规则版本 |
| **药物安全信号可复演窗口计算** | `signals/`：冻结病例版本截止线+术语归一版本；PRR/ROR（含95%CI）/χ² 四格指标与阈值判定；分片作业检查点续跑、任一分片失败不发布、同计算键并发单作业；迟到随访/归并拆解/因果修正在后续窗口撤销旧贡献并计新贡献（每来源每窗口一次）；信号修订 append-only、旧处置保留、新修订重新处置、过期修订号拒绝；输入/输出哈希 + 冻结输入重算核验 |

## 双人会签（药物安全决定）

药物安全决定（候选归并关系、严重等级变化、待发送监管报告）在发送前必须
经过**两名不同复核员对同一份不可变评审单、同一个数据修订号**会签。

**评审单（不可变）**：发起时把三类内容冻结进 `review_sheet_items`
（数据库触发器拒绝 UPDATE/DELETE）：待发送决定（`PENDING_REPORT`，含规则
`rule_id`/加急/截止日）、候选归并簇（`MERGE_CANDIDATE`，含分值理由）、
严重等级翻转（`SERIOUSNESS_CHANGE`）。按内容 SHA-256 去重，部分唯一索引
`ux_review_open_content` 保证并发重复发起只产生一张 OPEN 单。

**数据修订号（失效机制）**：`review_revision` 单行全局版本。新材料入库
（接收/随访）、归并确认、拆解、规则重新计算都在各自的 `BEGIN IMMEDIATE`
事务内 `bump_revision()`，并原子地把所有 OPEN 单置为 `INVALIDATED`
（记录 `NEW_MATERIAL/MERGED/SPLIT/RULE_RECALCULATED` 原因）。旧单的签署
意见（`review_events` 流水 + `review_signatures` 槽位）全部保留、可审计，
但永远不能再推动放行；再次发起只能引用最新修订号。

**会签规则**：

- 状态机单向：`OPEN → APPROVED | REJECTED | INVALIDATED`（均终态）；
- `review_signatures` 主键 `(sheet_id, reviewer_key)`：同一身份只能占一个
  意见槽，重复签署返回 409 `ALREADY_SIGNED`，**不累计票数**；身份键由
  `identity.py` 做 NFKC Unicode 等价折叠 + `casefold` 大小写归一 +
  仅去首尾空白，因此 `alice` / `ALICE` / `" alice "` / 全角 `Ａlice` /
  `Å`(U+212B) 与 `Å`(U+00C5) 都稳定判为同一复核员，单人无法靠表面写法
  差异占满两槽；两名不同复核员 APPROVED 才完成；任一人 VETOED 立即
  REJECTED 结束本轮；
- 空白 / 非字符串 / 超长身份在领取、释放、赞成、否决、撤回及待办/审计
  查询上一律明确拒绝（400 `BLANK_REVIEWER` / `INVALID_REVIEWER` /
  `REVIEWER_TOO_LONG`），绝不把缺失身份静默当作某个复核员；内部空白
  有意义（`"a b"` 与 `"ab"` 是两个人）；
- 每次提交的**原始字符串原样留痕**（签名 `reviewer`、事件 `actor`、
  放行 `approved_by_json`），规范化键 `reviewer_key` / `actor_key` /
  `lease_holder_key` 只用于身份判定与比较，全程可追溯；
- 撤回只对 OPEN 单的赞成意见有效（删槽，流水保留），否决为终态不可撤回；
- 全部动作（创建/领取/释放/赞成/否决/撤回/失效/租约到期）追加进只增的
  `review_events`，会签通过/否决同时镜像到病例审计哈希链。

**租约（待办协作）**：领取是带条件的原子 UPDATE（未持有或已到期才能成功），
多人抢领只有一人成功，其余 409；TTL 到期由 `expire_leases` 释放并留痕；
`ReviewService` 构造（服务恢复）时清理到期租约，**不产生任何业务结论**。
租约持有者按规范化身份键判定：同一复核员用任一等价写法领取是本人续租
（幂等），释放/待办可见性/到期留痕也都以同一身份键稳定比较。租约只影响
待办工作台可见性，不限制签署权（真正的并发安全由槽位主键保证）。

**一个生效结论**：抢领、同人重复签、批准与撤回/否决并发、服务恢复等所有
竞争都在共享 RLock + SQLite 写锁上串行，只可能有一个终态。批准完成时
`countersign_approvals` 以 `report_id` 为主键幂等解锁冻结的待发送决定；
已 ACKED/VOID 的冻结项跳过；修订后新排队的报告必须重新会签。

**发送闸门与兼容开关**：默认不改变既有行为；以
`--require-countersign` 启动后，未取得会签放行的报告在派发时返回
403 `COUNTERSIGN_REQUIRED`（后台派发线程同样遵守，只发送已放行项）。
原有无需会签的调用可在 `--allow-countersign-compat` 启动时，于派发请求中
显式带 `"allow_ungated": true` 继续使用（未在服务端开启兼容时该字段无效）。

```bash
# 强制双人会签启动；老调用可经显式兼容开关放行
python3 -m pv_service.serve --data data/pv --port 8080 --seed-demo \
    --require-countersign --allow-countersign-compat
```

典型流程：`POST /reviews` 冻结评审单 → 两名复核员分别
`/claim`（可选）、`/approve` → `POST /reports/dispatch` 发送；
任一人 `/veto` 本轮终止；评审期间随访/合并/拆分/规则重算自动作废旧单，
从 `GET /reviews/revisions` 可查失效原因，从 `GET /reviews/{id}` 可见
保留的全部历史意见。

## 药物安全法规规则变更：影响分析与受控启用

法规报送口径以**不可变、带签名、可追溯版本**的规则包管理。模块独立于
既有计算路径，但把「本次使用的规则版本」一路固定到报告行、会签冻结单
与发送回执，使旧规则与候选规则、旧结果与更正结果都可复核、不可混淆。

**规则包登记与三校验**（`rule_pkg.py` / `rule_gov.py`）：每个包携带
辖区、版本、半开生效区间 `[effective_from, effective_to)`、依赖版本、
内容摘要与报送天数口径（`serious/listed/routine_days`）。登记时：

1. *签名校验*：HMAC-SHA256（密钥按 `signer_id` 在只增密钥注册表解析，
   常量时间比较），签名不符/未知签名人拒绝；
2. *依赖校验*：`depends_on` 的 (package,version) 必须已登记；
3. *时间区间冲突*：同辖区任意两包生效区间不得相交（首尾相接合法）。

内容指纹 = 规范 JSON 的 SHA-256：**相同内容重复登记返回原版本**
（`deduped=true`）；**不同内容占用同一版本号** 由
`UNIQUE(jurisdiction,version)` 兜底拒绝（409 `VERSION_CONFLICT`）。
规则包行只增（触发器拒绝 UPDATE/DELETE）。

**试算（影响分析，纯只读）**：选择一组历史数据快照（显式病例/版本，
或按 `as_of/limit` 选最新版本），创建任务时把快照副本冻结进
`rule_trial_units`，再用基线版本（缺省 `builtin:legacy@0`）与候选版本
分别重算决定，逐分片落字段级差异与受影响病例集合。试算**不改病例、
不会签、不发送**；任务按 `(辖区,候选包,幂等键)` 唯一。

**受控启用绑定当前版本**：启用可带 `expected_package_id/version` 乐观
条件，调用方基于过期视图启用返回 409 `VERSION_MOVED`。并发启用/回滚/
定时在共享 RLock + 辖区状态行上串行；同 `idempotency_key` 重放返回同一
结论——**只有一个生效结果**。

**已发送 vs 未发送（关键合规边界）**：

- *未发送*（QUEUED/SENDING/FAILED）：启用成功后在单事务内按新版本重算
  决定/截止日并改写固定版本，**监管编号不变**；新版本判定无需报送则
  VOID（编号保留）。重算推进数据修订号，**作废旧会签单**，须以新决定
  重新双人会签才能发送；
- *已发送*（ACKED）：报告行、原始规则版本、历史结果**永不改动**，只
  生成一条可复核的 `rule_correction_candidates`（含原始/候选决定与差异），
  同 (报告,候选版本) 去重，绝不覆盖原记录。

**两阶段 + 检查点**：启用作业阶段一逐分片把新决定暂存到
`rule_job_units`（每个单元独立短事务），全部分片成功后阶段二在**单
BEGIN IMMEDIATE 事务**内原子应用并重写辖区版本指针。**任一分片失败 →
作业 FAILED、版本指针不动、无部分启用**；进程中断后 `recover` 把
RUNNING 任务重置为可续跑，从 `next_shard` 只补未完成单元，
**重复请求不重复生成差异、更正候选或审计事件**。定时生效由
`rule_schedules`（每辖区至多一个 PENDING）到点触发，FIRED 关联启用记录。

`rule_audit_events` 是一条全局只增哈希链（seq/prev_hash/entry_hash，
审计帧带幂等键），`GET /rules/audit/verify` 可离线重放核验。

```bash
# 后台自动续跑中断任务 + 触发到点定时生效
python3 -m pv_service.serve --data data/pv --port 8080 --seed-demo \
    --jurisdiction CN --auto-rule-scheduler
```

## 药物安全信号：可复演窗口计算（PRR/ROR）

分析员定义**药品集合、事件分组、观察期、最小样本量与 PRR/ROR 阈值**，
一次窗口运行同时**冻结病例版本截止线（cutoff）与术语归一版本**，输出
四格计数、指标、入选病例清单与信号判定。独立服务、独立库（`signal.db`），
纯标准库实现：`python -m pv_service.signals.serve --data data/signals --port 8090`。

**可复现性（同一配置+截止线 = 同一份结果）**

- 提交作业时在单事务内枚举窗口内每个来源在 `recorded_at < cutoff` 可见的
  最新版本（迟到版本不可见）与 `effective_at <= cutoff` 的当前病例归属，
  连同分片号、增量脏标记冻结进 `sig_job_sources`；分片只读冻结清单 +
  不可变版本表，作业运行期间到达的数据绝不混入。
- 术语归一目录（`sig_term_catalogs`）与监测定义版本
  （`sig_definition_versions`）均不可变；后台登记新版本不影响任何历史
  窗口。NFKC+casefold 折叠匹配；相同内容重复登记/定义改版返回原版本。
- 指标统一 6 位小数；来源行（组集合/标准代码/因果等级）与窗口输出按
  确定性顺序序列化做 SHA-256（`input_hash`/`output_hash`）；
  `POST /windows/{id}/verify` 用冻结输入独立重算逐行/逐组对比。
- `sig_jobs.computation_key` UNIQUE：并发提交同一计算键只产生一个作业、
  一个 run、一组信号修订（后来的提交返回既有作业 `deduped=true`）。

**后续窗口如何吸收修订（撤销旧贡献、计新贡献）**

- 病例版本/归属只增：迟到随访、因果评价修正产生新版本；归并/拆解是旧
  归属行 1→0 + 新行（部分唯一索引保证一来源一个当前病例）+ 血缘闭包。
- 计数单位是**来源**（同一来源在一个窗口只贡献一次）；`WITHDRAWAL`
  版本使来源退出后续窗口样本。跨窗口差异按来源行指纹对账：跨药品组/
  跨事件组/退出/进入逐来源列出（`/diffs/runs`），信号修订有字段级
  差异（`/diffs/revisions`），单来源轨迹见
  `/sources/{id}/contributions`（版本推进、CASE_RELINK、组迁移）。
- **全量回算与增量更新**：增量以最近已发布窗口为基线，指纹未变的来源
  直接复制已发布行，仅脏来源重算；两种模式产出经同一重算核验。
- 同一观察期新截止线的全量与增量是同一计算键（同一份结果）；不同窗口
  各自冻结、各自可复现。

**信号修订与分析员处置（发布后只追加）**

- `sig_runs` / `sig_run_cases` / `sig_run_pair_metrics` /
  `sig_signal_revisions` / `sig_signal_dispositions` 全部 append-only
  （SQLite 触发器拒绝改删），后台计算不能覆盖已发布快照。
- 四格/判定与上一修订**逐字节相同则复用修订号**（旧处置原样有效）；
  变化才追加 `NEW/COUNT_CHANGE/WITHDRAWN` 新修订，新修订恒为
  `PENDING_TRIAGE`，**必须重新确认/驳回/标记观察**。
- 对旧修订操作、或携带过期 `expected_seq_no` 一律 409 `STALE_REVISION`
  （响应携带当前修订号）；一个修订至多一条处置且不可修改。

**分片作业、崩溃恢复与半成品防线**

- 分片在独立短事务内 claim→构建→`COMMITTED`；暂存行 `(job,source)` UNIQUE。
  任一分片失败 → 分片/作业 FAILED，**绝不发布半成品指标**；
  `POST /recover` 复位失败分片（删其暂存行重做）、中断的 RUNNING 作业
  复位续跑，已提交分片与审计帧不重放（审计帧带幂等键）。
- `--auto-worker` 后台线程自动恢复并推进 PENDING 作业。
- 测试钩子可在分片提交前/后注入故障（见 `test_signals_jobs.py`）。

接口：术语目录、监测定义（版本）、来源（接收/随访/退出）、归并/拆解、
运行与进度、窗口结果（四格/指标/入选清单）、重算核验、来源贡献追踪、
运行/修订差异、信号确认驳回、恢复状态、审计查询与哈希链核验，共 30+ 个
HTTP 端点（见 `signals/api.py` 头部清单）。

```bash
# 运行信号测试（74 个：指标/判定、归一/定义、四格、可复现、信号修订、
# 迟到随访/归并/因果/退出、分片崩溃恢复、并发单作业、不可变/API/审计链）
python3 -m unittest discover -s tests -p 'test_signals_*.py' -v

# 端到端剧本（无需起服务）
python3 run_signal_demo.py

# 启动信号服务（独立端口）
python3 -m pv_service.signals.serve --data data/signals --port 8090 --auto-worker
```

## 证据包与离线核验

安全专员对**病例的某个版本**发起导出，得到一个自包含、可移交、带签名的 tar 包。

**包内五段证据 + 完整性锚点**

- `sources/*.json` 来源原文（医院/热线/企业 payload 原样）；
- `cases/*.json`、`export/snapshot.json` 发起时绑定的版本快照序列；
- `relations.json` 截止锚点时的合并/拆分关系（含被合并病例与拆分母病例血缘）；
- `reports/*.json` 报送记录、**截止时冻结状态**与监管回执 ack；
- `timeline/*.json` 各病例审计哈希链（`seq/prev_hash/entry_hash`）；
- `manifest.json`（清单：每文件 sha256/size + 链头）、`manifest.sig`（Ed25519）、
  `package.json`（签名公钥与指纹）。

**四条硬约束如何成立**

1. *快照绑定，不拼接*：导出锚定具体版本，整个收集在一个 `BEGIN IMMEDIATE`
   事务内按 rowid 截止线读取；导出期间的随访/合并/拆分只追加新版本/新行，
   绝不混进包里。引用悬空（来源/报告缺失）或审计链断裂 → 任务 `FAILED`
   （`SNAPSHOT_INCOMPLETE` / `AUDIT_CHAIN_BROKEN`），不落任何半成品包。
   报告状态由截止审计帧重放冻结，锚点之后才到达的 ACK 不会被拼进来。
2. *相同请求只留一份包*：`export_tasks(case_id, version_id, idempotency_key)`
   UNIQUE + 内容寻址文件（`packages/<manifest-root>.tar`）+ 确定性构建，
   并发请求、失败重试、进程重启都只产生同一任务、同一文件。
3. *崩溃恢复*：重启把 `BUILDING` 重置为 `PENDING` 重跑；包文件丢失的
   `COMPLETED` 任务重新排队；只清理被重置/无主的临时目录，不影响并发进程。
4. *离线核验逐项失败、绝不误报有效*：纯标准库 Ed25519（RFC 8032，
   与 OpenSSL 双向交叉验证）核验签名；重算清单根、每个文件摘要、审计链
   每一帧、版本状态、报告回执一致性、合并拆分关系。任何一项失败都在结果中
   逐条列出具体失败项，`valid=false`。可用带外公钥指纹 `--fingerprint` 绑定来源。

```bash
# 发起导出（HTTP）后下载；或直接离线核验（不连数据库/网络）
python3 -m pv_service.verify_offline package.tar                 # 退出码 0 有效 / 2 核验失败 / 3 无法打开
python3 -m pv_service.verify_offline package.tar --fingerprint <hex>  # 绑定带外信任公钥
python3 -m pv_service.verify_offline package.tar --json          # 完整结构化报告
```

## 快速开始

```bash
# 运行测试（308 个 = 原有 234 个 + 信号窗口计算 74 个；含并发、崩溃恢复、
# 幂等、证据包篡改/离线核验、双人会签、法规规则治理，以及信号可复现性/
# 分片续跑/同键单作业/信号修订追加/过期号拒绝/跨窗口差异/审计链）
python3 -m unittest discover -s tests -v

# 端到端剧本演示（无需起服务）
python3 run_demo.py

# 启动 HTTP 服务
python3 -m pv_service.serve --data data/pv --port 8080 --seed-demo
```

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/intake` | 来源报文接收（`source_system`+`external_id` 幂等） |
| GET | `/api/v1/cases` | 病例工作台（状态、来源、待复核簇、报告） |
| GET | `/api/v1/cases/{id}` | 病例详情：全部版本 + 完整时间线 |
| POST | `/api/v1/cases/{id}/followup` | 随访材料（产生新版本，自动判定是否报送/加急） |
| POST | `/api/v1/cases/{id}/split` | 将某来源拆分到新病例 |
| GET | `/api/v1/duplicates?status=PROPOSED` | 待复核疑似重复簇（含分值理由） |
| POST | `/api/v1/duplicates/{id}/confirm` | 专员确认合并（可指定 survivor） |
| POST | `/api/v1/duplicates/{id}/reject` | 专员否决（该边永不复活） |
| GET | `/api/v1/reports?status=` | 报告列表 |
| GET | `/api/v1/reports/due` | 待报送/逾期视图（加急优先排序） |
| POST | `/api/v1/reports/dispatch` | 同步派发待发送报告（强制会签时未放行返回 403；兼容模式可带 `allow_ungated`） |
| GET | `/api/v1/reports/{id}` | 报告详情（含监管回执 ack） |
| POST | `/api/v1/reports/{id}/requeue` | 失败报告人工重置重发（编号不变，会签闸门保留） |
| POST | `/api/v1/reviews` | 发起不可变会签评审单（`report_ids` 可选；冻结最新修订号） |
| GET | `/api/v1/reviews/todos` | 待办领取工作台（`?reviewer=`） |
| POST | `/api/v1/reviews/{id}/claim` | 领取待办租约（`{reviewer}`，多人抢领仅一人成功） |
| POST | `/api/v1/reviews/{id}/release` | 主动释放租约 |
| POST | `/api/v1/reviews/{id}/approve` | 赞成签署（`{reviewer,comment}`；两名不同人即放行） |
| POST | `/api/v1/reviews/{id}/veto` | 否决（任一人否决即结束本轮） |
| POST | `/api/v1/reviews/{id}/withdraw` | 撤回本人赞成（仅 OPEN 单） |
| POST | `/api/v1/reviews/{id}/expire` | 释放到期租约（维护/恢复） |
| GET | `/api/v1/reviews/{id}` | 评审单详情（冻结项/活意见/完整事件流水） |
| GET | `/api/v1/reviews/history` | 决定历史（`?report_id=&case_id=&status=`） |
| GET | `/api/v1/reviews/audit` | 会签审计查询（`?sheet_id=&event_type=&actor=`） |
| GET | `/api/v1/reviews/revisions` | 修订号推进与旧单失效原因历史 |
| GET | `/api/v1/reviews/approvals` | 会签放行记录（`?report_id=`） |
| POST | `/api/v1/rules/recalculate` | 规则重新计算（作废旧评审单，逐病例落审计） |
| POST | `/api/v1/rule-packages` | 登记不可变规则包（签名/依赖/区间三校验；相同内容返回原版本） |
| POST | `/api/v1/rule-packages/sign` | 用密钥对规则包内容生成 HMAC 签名（登记辅助） |
| GET | `/api/v1/rule-packages` | 规则包列表（`?jurisdiction=`） |
| GET | `/api/v1/rule-packages/{id}` | 规则包详情（含内容/指纹/签名/依赖） |
| GET | `/api/v1/rules/current` | 当前生效规则版本（`?jurisdiction=`，缺省内置 legacy） |
| POST | `/api/v1/rules/trials` | 创建试算任务（冻结历史快照集合，`candidate`/`baseline`/`selector`） |
| GET | `/api/v1/rules/trials` | 试算任务列表（`?jurisdiction=`） |
| GET | `/api/v1/rules/trials/{id}` | 试算任务状态 |
| POST | `/api/v1/rules/trials/{id}/run` | 从检查点续跑试算（`max_shards` 可选） |
| GET | `/api/v1/rules/trials/{id}/progress` | 进度 + 受影响病例/报告集合 |
| GET | `/api/v1/rules/trials/{id}/diffs` | 旧/候选决定差异（`?changed_only=&limit=`） |
| POST | `/api/v1/rules/activate` | 受控启用（`target`，可带 `expected_*` 绑定当前版本/幂等键） |
| POST | `/api/v1/rules/rollback` | 回滚（`target` 可省=上一版本） |
| GET | `/api/v1/rules/activations` | 启用/回滚/定时生效历史（`?jurisdiction=`） |
| GET | `/api/v1/rules/jobs` | 启用作业列表（`?jurisdiction=`） |
| GET | `/api/v1/rules/jobs/{id}` | 作业状态/分片进度/更正候选数 |
| POST | `/api/v1/rules/jobs/{id}/run` | 从检查点续跑启用作业 |
| GET | `/api/v1/rules/corrections` | 已发送报告的更正候选（`?report_id=&jurisdiction=`） |
| POST | `/api/v1/rules/schedules` | 创建定时生效计划（每辖区至多一个 PENDING） |
| GET | `/api/v1/rules/schedules` | 定时计划列表（`?status=`） |
| POST | `/api/v1/rules/schedules/{id}/cancel` | 取消定时计划 |
| POST | `/api/v1/rules/recover` | 崩溃恢复（重置中断任务）并续跑（`{"resume":true}`） |
| GET | `/api/v1/rules/audit` | 规则治理审计事件（`?event_type=&jurisdiction=`） |
| GET | `/api/v1/rules/audit/verify` | 核验治理审计哈希链 |
| GET | `/api/v1/cases/{id}/timeline` | 病例审计时间线（含 seq/prev_hash/entry_hash 哈希链） |
| POST | `/api/v1/exports` | 发起证据包导出（`case_id`+可选 `version_id`/`idempotency_key`，幂等；默认 `wait`） |
| GET | `/api/v1/exports` | 导出任务列表（可按 `?case_id=` 过滤） |
| GET | `/api/v1/exports/{id}` | 导出任务状态与构建进度 |
| GET | `/api/v1/exports/{id}/download` | 下载证据包（`application/x-tar`） |
| POST | `/api/v1/exports/{id}/verify` | 服务端离线核验（逐项结果并留档） |
| GET | `/api/v1/exports/{id}/verifications` | 该任务的历史核验结果 |

### 报文示例

```jsonc
// POST /api/v1/intake
{
  "source_system": "HOSPITAL",        // HOSPITAL | HOTLINE | ENTERPRISE
  "external_id": "H-2026-1001",
  "received_at": "2026-09-10T09:00:00",
  "payload": {
    "patient_name": "张三", "gender": "男", "dob": "1980-03-03",
    "patient_identifier": "ID-998",
    "drugs": [{"name": "布洛芬", "start_date": "2026-09-05", "causality": "可能"}],
    "reactions": ["皮疹"],
    "seriousness": "不严重", "event_date": "2026-09-07"
  }
}
```

同事件的热线报文可以是英文、别名（老张）、斜杠日期（2026/09/05）、
西语药名（ibuprofeno/sarpullido）——归一化后仍会识别为同事件。

## 关键设计说明

**编号安全为什么成立（三层）**
1. `number_sequence` 的 `INSERT ... ON CONFLICT DO UPDATE SET last=last+1`
   在与病例变更同一 `BEGIN IMMEDIATE` 事务内执行，SQLite 写事务全局串行；
2. `regulatory_reports.regulatory_number` 有 UNIQUE 约束 + 版本唯一索引
   （一个病例版本至多一份报告），重试只 UPDATE 状态行，永不重新分配编号；
3. 监管网关（`gateway.py`）以编号为幂等键，即使在“网关已受理、本地回执未落库”
   窗口崩溃，重启重放也只返回同一个 ack。作废报告（合并去重）编号同样永不复用。

**加急判定不是写死的**：规则是 `rules.py` 中按顺序求值的规则链，
天数（15/30/90）与 listedness 参考表均可配置；随访是否触发报送取决于
`significant_changes()` 对用药时间/严重性/因果性/转归/新增药械反应的差异分析。

**拆分并发**：来源转移 = 停用旧归属行 + 插入新归属行，部分唯一索引保证
数据库层面不可能出现双归属；8 线程并发拆分同一来源只有 1 个成功（有测试）。

## 模块

```
pv_service/
  db.py             表结构、不可变触发器、IMMEDIATE 事务、旧库审计链迁移
  normalize.py      多语言/别名/严重性/因果归一化、宽松日期
  dedup.py          加权打分、强标识短路、并查集聚类
  rules.py          15/30/90 日规则链、随访实质变化分析
  rule_pkg.py       规则包内容规范、HMAC 签名、生效区间、内容指纹、版本化引擎
  rule_audit.py     规则治理全局审计哈希链（登记/启用/回滚/定时/试算/恢复）
  rule_gov.py       登记三校验、历史快照试算、受控启用/回滚/定时、两阶段
                    分片+检查点续跑、已发送更正候选、幂等、乐观并发
  revision.py       数据修订号原语：业务事务内推进并原子作废旧评审单
  identity.py       复核员身份规范化（NFKC+casefold+首尾空白）与空白/非法拒绝
  countersign.py    双人会签：不可变评审单、租约、签署/否决/撤回、放行闸门、审计
  service.py        病例工作台：接收、随访、合并、拆分、血缘、时间线、编号分配、规则重算
  reporting.py      报告状态机、退避重试、崩溃恢复、后台派发
  gateway.py        模拟监管网关（幂等回执、可脚本化故障）
  audit.py          病例审计哈希链（seq/prev_hash/entry_hash，逐帧核验）
  ed25519.py        纯 Python Ed25519（RFC 8032，零第三方依赖）
  package_codec.py  确定性 tar 证据包 + 清单根 + 签名
  export_service.py 导出任务：版本快照绑定、去重、崩溃恢复、内容寻址落盘
  verifier.py       离线核验纯函数（逐项失败，绝不误报有效）
  verify_offline.py 离线核验命令行（另一进程，不连数据库）
  signals/          药物安全信号可复演窗口计算（独立 signal.db/端口）：
    db.py             信号子系统 schema 与 append-only/归属唯一触发器
    metrics.py        PRR/ROR（log-Wald 95%CI）/χ² 纯函数与阈值判定
    catalog.py        不可变术语归一版本（NFKC+casefold 折叠匹配）
    definitions.py    监测定义不可变版本（药品组/事件组/阈值/因果排除）
    build.py          冻结来源版本 -> 来源行（组归属/指纹）-> 四格聚合
    sources.py        来源版本接收（迟到随访/退出）、归并拆解血缘、as-of 解析
    runner.py         提交冻结/分片检查点/原子发布/信号修订/重算核验/恢复
    signals.py        信号确认/驳回/观察（旧结论保留、过期修订号拒绝）
    results.py        窗口结果、入选清单、跨窗口贡献追踪、修订差异
    audit.py          信号子系统全局审计哈希链
    service.py        应用装配 + 后台恢复续跑工作线程
    api.py / serve.py 标准库 http.server JSON 接口 / 启动入口（默认 8090）
  app.py            装配（共享锁、参考数据）
  api.py            标准库 http.server 的 JSON 接口
tests/              308 个测试（归一化/规则、去重、不可变、合并拆分、报送、
                     并发、API、重启恢复、审计链、Ed25519、证据包导出/篡改/离线核验、
                     双人会签核心/并发租约/HTTP 接口/兼容开关/
                     复核员身份规范化/旧库身份迁移/
                     法规规则包/治理/崩溃续跑/治理并发/治理 HTTP；
                     test_signals_*：指标判定、归一/定义/构建、四格窗口、
                     可复现/冻结、信号修订/过期号、迟到随访/归并/因果/退出、
                     分片崩溃恢复/并发单作业、不可变触发器/HTTP/审计链）
```
