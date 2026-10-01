"""信号监测子系统的 SQLite 持久化（独立 signal.db）。

与主库（pv.db）风格一致：不可变事实表 + 触发器兜底、归属只增、审计哈希链。
信号计算的关键持久化不变量：

1. ``sig_term_catalogs`` 术语归一版本登记后不可变（一次运行冻结的归一版本
   永不被后台新版本改写）。
2. ``sig_definition_versions`` 监测定义只增：分析员改药品集合/事件分组/
   阈值产生新版本，旧版本保留可复算。
3. ``sig_source_versions`` 病例版本只增：迟到随访、归并拆解、因果评价
   修正都产生新版本而非覆盖；截止线之后的版本对当次运行不可见。
4. ``sig_case_links`` 来源归属只增（旧行 1→0 + 新行），部分唯一索引
   保证一个来源任何时刻只归属一个当前病例。
5. ``sig_runs`` / ``sig_run_cases`` / ``sig_run_pair_metrics`` /
   ``sig_signal_revisions`` / ``sig_signal_dispositions`` 全部只增——
   发布后的窗口快照与信号快照只能被新修订追加，不能被后台计算覆盖。
6. 作业分片表以 ``(job_id, shard_no)`` 记录提交状态；staging 行按
   ``(job_id, source_id)`` 唯一，崩溃后只补未提交分片，且不重复贡献。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

--------------------------------------------------------------------------------
-- 术语归一版本（不可变）
--------------------------------------------------------------------------------
-- 一次运行冻结 term_catalog_id：事件术语 -> 标准概念（及可选的药品名称
-- -> 药品代码）映射全部来自该版本快照，后台再登记新版本不影响历史运行。
CREATE TABLE IF NOT EXISTS sig_term_catalogs (
    id           TEXT PRIMARY KEY,            -- 分析员给定的版本标识，如 meddra-like@2026Q3
    name         TEXT NOT NULL,
    spec_json    TEXT NOT NULL,               -- {concepts:[{code,name,kind,terms:[...]}]}
    content_hash TEXT NOT NULL UNIQUE,        -- 相同内容重复登记返回原版本
    created_by   TEXT,
    created_at   TEXT NOT NULL
);

--------------------------------------------------------------------------------
-- 监测定义（药品集合 / 事件分组 / 观察期默认值 / 最小样本量 / PRR/ROR 阈值）
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sig_definitions (
    id                 TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    current_version_no INTEGER NOT NULL DEFAULT 0,
    created_by         TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);

-- 定义版本（不可变；改分组/阈值 = 新版本）。spec_json 结构：
-- {
--   "drug_groups":  [{"id","name","drug_codes":[...]}],
--   "event_groups": [{"id","name","concept_codes":[...]}],
--   "thresholds":   {"min_cases":3,"min_prr":2.0,"min_prr_ci_lower":1,
--                    "min_ror":2.0,"min_ror_ci_lower":1,"min_chi_square":3.841},
--   "excluded_causality_levels": ["UNRELATED"]
-- }
CREATE TABLE IF NOT EXISTS sig_definition_versions (
    id            TEXT PRIMARY KEY,
    definition_id TEXT NOT NULL REFERENCES sig_definitions(id),
    version_no    INTEGER NOT NULL,
    spec_json     TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    created_by    TEXT,
    created_at    TEXT NOT NULL,
    UNIQUE(definition_id, version_no)
);
CREATE INDEX IF NOT EXISTS ix_sig_defver_def ON sig_definition_versions(definition_id);

--------------------------------------------------------------------------------
-- 病例来源（报告单位，四格计数按来源去重）
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sig_sources (
    id          TEXT PRIMARY KEY,
    origin      TEXT NOT NULL,                -- HOSPITAL / HOTLINE / ENTERPRISE / ...
    external_id TEXT NOT NULL,
    active      INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
    withdrawn   INTEGER NOT NULL DEFAULT 0 CHECK (withdrawn IN (0,1)),
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    UNIQUE(origin, external_id)
);

-- 来源版本（不可变）。迟到随访/因果评价修正/退出样本都产生新版本；
-- change_type=WITHDRAWAL 的版本表示该来源退出后续窗口样本。
CREATE TABLE IF NOT EXISTS sig_source_versions (
    id               TEXT PRIMARY KEY,
    source_id        TEXT NOT NULL REFERENCES sig_sources(id),
    version_no       INTEGER NOT NULL,
    change_type      TEXT NOT NULL
                     CHECK (change_type IN ('INTAKE','FOLLOWUP','WITHDRAWAL')),
    event_date       TEXT NOT NULL,           -- 报告锚定日期（观察期入选判定）
    payload_json     TEXT NOT NULL,           -- 完整快照（药品/事件/因果/严重度）
    content_hash     TEXT NOT NULL,
    idempotency_key  TEXT,
    recorded_at      TEXT NOT NULL,           -- 版本到达时间（截止线可见性判定）
    created_at       TEXT NOT NULL,
    UNIQUE(source_id, version_no)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_sig_sv_idem
    ON sig_source_versions(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_sig_sv_cutoff
    ON sig_source_versions(source_id, recorded_at, version_no);

-- 病例（归并后的聚合根；来源是计数与贡献追踪的最小单位）
CREATE TABLE IF NOT EXISTS sig_cases (
    id               TEXT PRIMARY KEY,
    status           TEXT NOT NULL DEFAULT 'OPEN'
                     CHECK (status IN ('OPEN','MERGED','SPLIT_OFF')),
    created_at       TEXT NOT NULL,
    superseded_by_id TEXT REFERENCES sig_cases(id)
);

-- 来源 <-> 当前病例归属（只增；归并/拆解 = 旧行 1->0 + 新行）
CREATE TABLE IF NOT EXISTS sig_case_links (
    id           TEXT PRIMARY KEY,
    source_id    TEXT NOT NULL REFERENCES sig_sources(id),
    case_id      TEXT NOT NULL REFERENCES sig_cases(id),
    active       INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
    reason       TEXT NOT NULL CHECK (reason IN ('INTAKE','MERGE','SPLIT')),
    effective_at TEXT NOT NULL,               -- 归属生效时刻（as-of 解析用）
    actor        TEXT,
    created_at   TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_sig_link_active
    ON sig_case_links(source_id) WHERE active = 1;
CREATE INDEX IF NOT EXISTS ix_sig_link_case ON sig_case_links(case_id) WHERE active = 1;

-- 病例血缘闭包（归并/拆解后历史病例可从存活病例追溯）
CREATE TABLE IF NOT EXISTS sig_case_lineage (
    ancestor_id   TEXT NOT NULL REFERENCES sig_cases(id),
    descendant_id TEXT NOT NULL REFERENCES sig_cases(id),
    depth         INTEGER NOT NULL,
    PRIMARY KEY (ancestor_id, descendant_id)
);

--------------------------------------------------------------------------------
-- 计算作业（分片 + 检查点）
--------------------------------------------------------------------------------
-- computation_key 全局唯一：并发提交同一计算键只产生一个作业，
-- 重复提交返回既有作业（完成后即同一份窗口结果与同一组信号修订）。
CREATE TABLE IF NOT EXISTS sig_jobs (
    id                    TEXT PRIMARY KEY,
    computation_key       TEXT NOT NULL UNIQUE,
    definition_id         TEXT NOT NULL,
    definition_version_id TEXT NOT NULL,
    definition_version_no INTEGER NOT NULL,
    term_catalog_id       TEXT NOT NULL,
    window_start          TEXT NOT NULL,
    window_end            TEXT NOT NULL,       -- 半开 [start, end)
    cutoff                TEXT NOT NULL,       -- 病例版本截止线（recorded_at < cutoff 可见）
    mode                  TEXT NOT NULL CHECK (mode IN ('FULL','INCREMENTAL')),
    prior_run_id          TEXT REFERENCES sig_runs(id),
    shard_count           INTEGER NOT NULL,
    status                TEXT NOT NULL
                          CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED')),
    run_id                TEXT REFERENCES sig_runs(id),
    total_sources         INTEGER NOT NULL DEFAULT 0,
    input_hash            TEXT,
    output_hash           TEXT,
    error_code            TEXT,
    error_message         TEXT,
    requested_by          TEXT,
    created_at            TEXT NOT NULL,
    started_at            TEXT,
    finished_at           TEXT
);
CREATE INDEX IF NOT EXISTS ix_sig_jobs_status ON sig_jobs(status);

-- 作业在提交事务内冻结的候选来源清单（含 as-of 版本与归属、分片号）。
-- 分片执行只读本表与不可变版本表：作业运行期间到达的迟到版本绝不混入。
CREATE TABLE IF NOT EXISTS sig_job_sources (
    job_id          TEXT NOT NULL REFERENCES sig_jobs(id),
    shard_no        INTEGER NOT NULL,
    source_id       TEXT NOT NULL,
    version_id      TEXT NOT NULL,
    version_no      INTEGER NOT NULL,
    case_id         TEXT NOT NULL,
    event_date      TEXT NOT NULL,
    prior_row_hash  TEXT,                  -- 增量模式：上一窗口该来源的行指纹
    dirty           INTEGER NOT NULL DEFAULT 1 CHECK (dirty IN (0,1)),
    PRIMARY KEY (job_id, source_id)
);
CREATE INDEX IF NOT EXISTS ix_sig_jobsrc_shard ON sig_job_sources(job_id, shard_no);

-- 分片提交状态检查点（崩溃恢复只补未 COMMITTED 分片）。
CREATE TABLE IF NOT EXISTS sig_job_shards (
    job_id        TEXT NOT NULL REFERENCES sig_jobs(id),
    shard_no      INTEGER NOT NULL,
    status        TEXT NOT NULL
                  CHECK (status IN ('PENDING','COMMITTED','FAILED')),
    source_count  INTEGER NOT NULL DEFAULT 0,
    error_code    TEXT,
    error_message TEXT,
    committed_at  TEXT,
    PRIMARY KEY (job_id, shard_no)
);

-- 分片产出的暂存行（每来源一行；唯一约束保证续跑/重放不重复贡献）。
CREATE TABLE IF NOT EXISTS sig_job_case_rows (
    job_id                TEXT NOT NULL REFERENCES sig_jobs(id),
    shard_no              INTEGER NOT NULL,
    source_id             TEXT NOT NULL,
    version_id            TEXT NOT NULL,
    version_no            INTEGER NOT NULL,
    case_id               TEXT NOT NULL,
    event_date            TEXT NOT NULL,
    drug_group_ids_json   TEXT NOT NULL DEFAULT '[]',
    event_group_ids_json  TEXT NOT NULL DEFAULT '[]',
    drug_codes_json       TEXT NOT NULL DEFAULT '[]',
    event_codes_json      TEXT NOT NULL DEFAULT '[]',
    causality_levels_json TEXT NOT NULL DEFAULT '[]',
    included              INTEGER NOT NULL CHECK (included IN (0,1)),
    exclude_reason        TEXT,
    row_hash              TEXT NOT NULL,
    PRIMARY KEY (job_id, source_id)
);
CREATE INDEX IF NOT EXISTS ix_sig_row_shard ON sig_job_case_rows(job_id, shard_no);

--------------------------------------------------------------------------------
-- 已发布窗口（不可变快照）
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sig_runs (
    id                    TEXT PRIMARY KEY,
    job_id                TEXT NOT NULL UNIQUE,
    computation_key       TEXT NOT NULL UNIQUE,
    definition_id         TEXT NOT NULL,
    definition_version_id TEXT NOT NULL,
    definition_version_no INTEGER NOT NULL,
    term_catalog_id       TEXT NOT NULL,
    window_start          TEXT NOT NULL,
    window_end            TEXT NOT NULL,
    cutoff                TEXT NOT NULL,
    mode                  TEXT NOT NULL,
    prior_run_id          TEXT,
    total_sources         INTEGER NOT NULL DEFAULT 0,
    included_sources      INTEGER NOT NULL DEFAULT 0,
    input_hash            TEXT NOT NULL,
    output_hash           TEXT NOT NULL,       -- 同一配置+截止线复算必须逐字节一致
    manifest_json         TEXT NOT NULL DEFAULT '{}',
    published_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_sig_runs_def ON sig_runs(definition_id, window_start);

-- 入选（及落选，included=0）来源清单：病例贡献追踪的权威快照。
CREATE TABLE IF NOT EXISTS sig_run_cases (
    run_id                TEXT NOT NULL REFERENCES sig_runs(id),
    source_id             TEXT NOT NULL,
    version_id            TEXT NOT NULL,
    version_no            INTEGER NOT NULL,
    case_id               TEXT NOT NULL,
    event_date            TEXT NOT NULL,
    drug_group_ids_json   TEXT NOT NULL DEFAULT '[]',
    event_group_ids_json  TEXT NOT NULL DEFAULT '[]',
    drug_codes_json       TEXT NOT NULL DEFAULT '[]',
    event_codes_json      TEXT NOT NULL DEFAULT '[]',
    causality_levels_json TEXT NOT NULL DEFAULT '[]',
    included              INTEGER NOT NULL CHECK (included IN (0,1)),
    exclude_reason        TEXT,
    row_hash              TEXT NOT NULL,
    PRIMARY KEY (run_id, source_id)
);
CREATE INDEX IF NOT EXISTS ix_sig_runcase_pair
    ON sig_run_cases(run_id, case_id);

-- 每个 (药品组, 事件组) 的四格计数 + 指标 + 判定（覆盖定义中全部组对）。
CREATE TABLE IF NOT EXISTS sig_run_pair_metrics (
    run_id          TEXT NOT NULL REFERENCES sig_runs(id),
    drug_group_id   TEXT NOT NULL,
    event_group_id  TEXT NOT NULL,
    a               INTEGER NOT NULL,
    b               INTEGER NOT NULL,
    c               INTEGER NOT NULL,
    d               INTEGER NOT NULL,
    n               INTEGER NOT NULL,
    metrics_json    TEXT NOT NULL,
    decision_json   TEXT NOT NULL,
    a_source_ids_json TEXT NOT NULL DEFAULT '[]',  -- a 格入选来源清单
    content_hash    TEXT NOT NULL,
    revision_id     TEXT REFERENCES sig_signal_revisions(id),
    PRIMARY KEY (run_id, drug_group_id, event_group_id)
);

--------------------------------------------------------------------------------
-- 信号修订（append-only；发布快照只能追加，不能覆盖）
--------------------------------------------------------------------------------
-- 同一 signal_key（定义|药品组|事件组）的修订号单调递增；
-- 新窗口若该组对四格/判定与上一修订完全相同则复用，不产生新修订。
CREATE TABLE IF NOT EXISTS sig_signal_revisions (
    id                 TEXT PRIMARY KEY,
    signal_key         TEXT NOT NULL,
    definition_id      TEXT NOT NULL,
    drug_group_id      TEXT NOT NULL,
    event_group_id     TEXT NOT NULL,
    seq_no             INTEGER NOT NULL,
    run_id             TEXT NOT NULL REFERENCES sig_runs(id),
    prev_revision_id   TEXT REFERENCES sig_signal_revisions(id),
    change_type        TEXT NOT NULL
                       CHECK (change_type IN ('NEW','COUNT_CHANGE','WITHDRAWN')),
    counts_json        TEXT NOT NULL,           -- {a,b,c,d,n}
    metrics_json       TEXT NOT NULL,
    decision_json      TEXT NOT NULL,
    content_hash       TEXT NOT NULL,
    created_at         TEXT NOT NULL,
    UNIQUE(signal_key, seq_no)
);
CREATE INDEX IF NOT EXISTS ix_sig_rev_key ON sig_signal_revisions(signal_key, seq_no);
CREATE INDEX IF NOT EXISTS ix_sig_rev_run ON sig_signal_revisions(run_id);

-- 分析员处置（append-only）：每个修订至多一条结论；
-- 旧修订的 CONFIRMED/REJECTED/WATCH 永久保留，新修订必须重新处置。
CREATE TABLE IF NOT EXISTS sig_signal_dispositions (
    id           TEXT PRIMARY KEY,
    revision_id  TEXT NOT NULL UNIQUE REFERENCES sig_signal_revisions(id),
    signal_key   TEXT NOT NULL,
    seq_no       INTEGER NOT NULL,
    decision     TEXT NOT NULL
                 CHECK (decision IN ('CONFIRMED','REJECTED','WATCH')),
    analyst      TEXT NOT NULL,
    comment      TEXT,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_sig_disp_key ON sig_signal_dispositions(signal_key, seq_no);

--------------------------------------------------------------------------------
-- 信号子系统审计哈希链（全局只增）
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sig_audit_events (
    id              TEXT PRIMARY KEY,
    seq             INTEGER NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    actor           TEXT,
    summary         TEXT NOT NULL,
    payload_json    TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT UNIQUE,
    prev_hash       TEXT NOT NULL,
    entry_hash      TEXT NOT NULL,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_sig_audit_type ON sig_audit_events(event_type, seq);

--------------------------------------------------------------------------------
-- 不可变性触发器
--------------------------------------------------------------------------------
CREATE TRIGGER IF NOT EXISTS trg_sig_catalog_no_update BEFORE UPDATE ON sig_term_catalogs
BEGIN SELECT RAISE(FAIL, 'sig_term_catalogs is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sig_catalog_no_delete BEFORE DELETE ON sig_term_catalogs
BEGIN SELECT RAISE(FAIL, 'sig_term_catalogs is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_sig_defver_no_update BEFORE UPDATE ON sig_definition_versions
BEGIN SELECT RAISE(FAIL, 'sig_definition_versions is append-only (create a new version)'); END;
CREATE TRIGGER IF NOT EXISTS trg_sig_defver_no_delete BEFORE DELETE ON sig_definition_versions
BEGIN SELECT RAISE(FAIL, 'sig_definition_versions is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_sig_sv_no_update BEFORE UPDATE ON sig_source_versions
BEGIN SELECT RAISE(FAIL, 'sig_source_versions is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sig_sv_no_delete BEFORE DELETE ON sig_source_versions
BEGIN SELECT RAISE(FAIL, 'sig_source_versions is append-only'); END;

-- 归属行只允许 active 1->0，其他字段改动一律拒绝
CREATE TRIGGER IF NOT EXISTS trg_sig_link_guard BEFORE UPDATE ON sig_case_links
WHEN NOT (
    NEW.id IS OLD.id AND NEW.source_id IS OLD.source_id AND NEW.case_id IS OLD.case_id
    AND NEW.reason IS OLD.reason AND NEW.effective_at IS OLD.effective_at
    AND NEW.actor IS OLD.actor AND NEW.created_at IS OLD.created_at
    AND OLD.active = 1 AND NEW.active = 0
)
BEGIN SELECT RAISE(FAIL, 'sig_case_links is append-only; only active 1->0 allowed'); END;
CREATE TRIGGER IF NOT EXISTS trg_sig_link_no_delete BEFORE DELETE ON sig_case_links
BEGIN SELECT RAISE(FAIL, 'sig_case_links is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_sig_lineage_no_update BEFORE UPDATE ON sig_case_lineage
BEGIN SELECT RAISE(FAIL, 'sig_case_lineage is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sig_lineage_no_delete BEFORE DELETE ON sig_case_lineage
BEGIN SELECT RAISE(FAIL, 'sig_case_lineage is append-only'); END;

-- 已发布窗口快照：只增（后台计算只能产生新 run / 新信号修订）
CREATE TRIGGER IF NOT EXISTS trg_sig_runs_no_update BEFORE UPDATE ON sig_runs
BEGIN SELECT RAISE(FAIL, 'sig_runs are immutable published snapshots'); END;
CREATE TRIGGER IF NOT EXISTS trg_sig_runs_no_delete BEFORE DELETE ON sig_runs
BEGIN SELECT RAISE(FAIL, 'sig_runs are immutable published snapshots'); END;

CREATE TRIGGER IF NOT EXISTS trg_sig_runcases_no_update BEFORE UPDATE ON sig_run_cases
BEGIN SELECT RAISE(FAIL, 'sig_run_cases is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sig_runcases_no_delete BEFORE DELETE ON sig_run_cases
BEGIN SELECT RAISE(FAIL, 'sig_run_cases is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_sig_pairm_no_update BEFORE UPDATE ON sig_run_pair_metrics
BEGIN SELECT RAISE(FAIL, 'sig_run_pair_metrics is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sig_pairm_no_delete BEFORE DELETE ON sig_run_pair_metrics
BEGIN SELECT RAISE(FAIL, 'sig_run_pair_metrics is append-only'); END;

-- 信号修订与处置：只增（发布后的信号只能追加新修订）
CREATE TRIGGER IF NOT EXISTS trg_sig_rev_no_update BEFORE UPDATE ON sig_signal_revisions
BEGIN SELECT RAISE(FAIL, 'sig_signal_revisions is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sig_rev_no_delete BEFORE DELETE ON sig_signal_revisions
BEGIN SELECT RAISE(FAIL, 'sig_signal_revisions is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_sig_disp_no_update BEFORE UPDATE ON sig_signal_dispositions
BEGIN SELECT RAISE(FAIL, 'sig_signal_dispositions is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sig_disp_no_delete BEFORE DELETE ON sig_signal_dispositions
BEGIN SELECT RAISE(FAIL, 'sig_signal_dispositions is append-only'); END;

-- 作业冻结的候选来源清单不可变
CREATE TRIGGER IF NOT EXISTS trg_sig_jobsrc_no_update BEFORE UPDATE ON sig_job_sources
BEGIN SELECT RAISE(FAIL, 'sig_job_sources is frozen at submission'); END;
CREATE TRIGGER IF NOT EXISTS trg_sig_jobsrc_no_delete BEFORE DELETE ON sig_job_sources
BEGIN SELECT RAISE(FAIL, 'sig_job_sources is frozen at submission'); END;

-- 审计链：只增
CREATE TRIGGER IF NOT EXISTS trg_sig_audit_no_update BEFORE UPDATE ON sig_audit_events
BEGIN SELECT RAISE(FAIL, 'sig_audit_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sig_audit_no_delete BEFORE DELETE ON sig_audit_events
BEGIN SELECT RAISE(FAIL, 'sig_audit_events is append-only'); END;
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    """打开一个数据库连接（短连接，线程间不共享）。"""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=15, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def initialize(db_path: str | Path) -> None:
    """建库建表（可重复执行）。"""
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection):
    """BEGIN IMMEDIATE 写事务：所有写者在数据库层串行化。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
