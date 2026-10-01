"""窗口计算作业：提交冻结 -> 分片续跑 -> 原子发布 -> 信号修订。

可复现性如何成立
----------------
1. **提交时冻结**：作业在单个 ``BEGIN IMMEDIATE`` 事务内解析定义版本、
   术语版本，并枚举观察期内每个来源在 ``cutoff`` 可见的版本与归属，
   连同分片号、增量脏标记写入 ``sig_job_sources``。分片执行只读这份
   冻结清单 + 不可变版本表，作业期间到达的迟到数据绝不混入。
2. **确定性构建**：来源行由 ``build.build_source_row`` 纯函数产生
   （组集合排序、代码排序、6 位小数指标）；输出按确定性顺序排列后做
   SHA-256。同一配置+截止线再次计算（包括全量 vs 增量）输出哈希一致。
3. **同键单作业**：``sig_jobs.computation_key`` UNIQUE；并发提交在
   SQLite 写锁上串行，后者命中既有作业，只产生一个作业与一组信号修订。

分片与发布
----------
- 每个分片独立短事务（先 claim 再执行再标记 COMMITTED），staging 行
  ``(job_id, source_id)`` 唯一；崩溃重启从已提交分片继续，不重复贡献。
- 任一分片失败 -> 分片 FAILED、作业 FAILED，**不发布任何窗口指标**
  （最终发布是单事务：读全部暂存 -> 聚合 -> 写 run/行/指标/信号修订）。
"""
from __future__ import annotations

import hashlib
import json
import threading
import uuid
from typing import Any

from ..service import Clock
from . import audit
from .build import aggregate_pairs, build_source_row
from .catalog import TermCatalog
from .db import connect, transaction
from .definitions import DefinitionService
from .sources import SourceService


def _now(clock: Clock) -> str:
    return clock.now().isoformat(timespec="seconds")


def _uid() -> str:
    return uuid.uuid4().hex


def _dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class RunnerError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class ShardFailed(RuntimeError):
    """测试钩子模拟的分片失败（作业停在 FAILED，无任何发布）。"""


def computation_key(*, definition_version_id: str, term_catalog_id: str,
                    window_start: str, window_end: str, cutoff: str) -> str:
    raw = _dumps({
        "definition_version_id": definition_version_id,
        "term_catalog_id": term_catalog_id,
        "window_start": window_start,
        "window_end": window_end,
        "cutoff": cutoff,
    })
    return _sha(raw)[:32]


def input_manifest(*, definition_spec: dict, term_catalog_id: str,
                   window_start: str, window_end: str, cutoff: str,
                   mode: str, frozen: list[dict]) -> dict:
    """作业输入指纹的完整内容（也用于审计/核验展示）。"""
    return {
        "definition": definition_spec,
        "term_catalog_id": term_catalog_id,
        "window": {"start": window_start, "end": window_end, "cutoff": cutoff},
        "mode": mode,
        "frozen_sources": sorted(
            ({"source_id": f["source_id"], "version_id": f["version_id"],
              "version_no": f["version_no"], "case_id": f["case_id"],
              "event_date": f["event_date"],
              "prior_row_hash": f.get("prior_row_hash"),
              "dirty": f.get("dirty", 1)} for f in frozen),
            key=lambda x: x["source_id"]),
    }


def _pair_content(pair: dict) -> dict:
    return {
        "a": pair["a"], "b": pair["b"], "c": pair["c"], "d": pair["d"],
        "n": pair["n"],
        "metrics": {k: pair["counts"][k] for k in
                    ("prr", "prr_ci95_lower", "prr_ci95_upper", "ror",
                     "ror_ci95_lower", "ror_ci95_upper", "chi_square")},
        "decision": pair["decision"]["decision"],
    }


def _pair_hash(pair: dict) -> str:
    return _sha(_dumps(_pair_content(pair)))


class SignalRunner:
    def __init__(self, db_path: str, clock: Clock,
                 shared_lock: threading.RLock | None = None,
                 shard_size: int = 50, crash_hook=None):
        self.db_path = db_path
        self.clock = clock
        self._lock = shared_lock or threading.RLock()
        self.shard_size = shard_size
        # 测试钩子：(phase, job_id, shard_no) -> 抛异常，模拟崩溃/分片失败。
        self.crash_hook = crash_hook

    def _conn(self):
        return connect(self.db_path)

    # ------------------------------------------------------------------
    # 提交作业（冻结）
    # ------------------------------------------------------------------
    def submit(self, *, definition_id: str, window_start: str, window_end: str,
               cutoff: str, term_catalog_id: str | None = None,
               definition_version_no: int | None = None,
               mode: str = "FULL", requested_by: str | None = None,
               shard_size: int | None = None) -> dict:
        """提交一次窗口计算（同计算键并发/重试只产生一个作业）。"""
        if window_end <= window_start:
            raise RunnerError("BAD_WINDOW", "观察期必须满足 start < end")
        if cutoff < window_start:
            raise RunnerError("BAD_CUTOFF", "截止线不能早于观察期开始")
        if mode not in ("FULL", "INCREMENTAL"):
            raise RunnerError("BAD_MODE", "mode 必须是 FULL 或 INCREMENTAL")
        shard_size = shard_size or self.shard_size
        with self._lock, transaction(self._conn()) as conn:
            drow = conn.execute(
                "SELECT current_version_no FROM sig_definitions WHERE id=?",
                (definition_id,)).fetchone()
            if not drow:
                raise RunnerError("DEFINITION_NOT_FOUND", f"定义不存在: {definition_id}")
            vno = definition_version_no or drow["current_version_no"]
            vrow = conn.execute(
                "SELECT id FROM sig_definition_versions WHERE definition_id=? "
                "AND version_no=?", (definition_id, vno)).fetchone()
            if not vrow:
                raise RunnerError("VERSION_NOT_FOUND",
                                  f"定义 {definition_id} 无版本 v{vno}")
            definition_version_id = vrow["id"]
            if term_catalog_id is None:
                raise RunnerError("CATALOG_REQUIRED",
                                  "提交必须显式冻结 term_catalog_id")
            if not conn.execute("SELECT 1 FROM sig_term_catalogs WHERE id=?",
                                (term_catalog_id,)).fetchone():
                raise RunnerError("CATALOG_NOT_FOUND",
                                  f"术语目录不存在: {term_catalog_id}")

            key = computation_key(
                definition_version_id=definition_version_id,
                term_catalog_id=term_catalog_id,
                window_start=window_start, window_end=window_end, cutoff=cutoff)
            existing = conn.execute(
                "SELECT id,status,run_id,mode FROM sig_jobs WHERE computation_key=?",
                (key,)).fetchone()
            if existing:
                job = self.get_job(existing["id"], conn=conn)
                job["deduped"] = True
                return job

            # 增量基线：同一观察期优先（同窗口新截止线），否则取相邻前一窗口；
            # 全量/增量运行都可作基线（增量基线的基线必已含全部历史数据）。
            prior_run_id = None
            if mode == "INCREMENTAL":
                prior = conn.execute(
                    "SELECT id FROM sig_runs WHERE definition_id=? AND cutoff <= ? "
                    "ORDER BY (window_start=?) DESC, cutoff DESC, window_start DESC, "
                    "published_at DESC LIMIT 1",
                    (definition_id, cutoff, window_start)).fetchone()
                if not prior:
                    raise RunnerError("NO_PRIOR_RUN",
                                      "增量更新需要该定义已有已发布的基线窗口")
                prior_run_id = prior["id"]

            # 枚举候选来源：观察期半开区间 + 截止线可见版本 + 截止线归属
            sources = conn.execute(
                "SELECT id FROM sig_sources WHERE active=1 ORDER BY id").fetchall()
            frozen: list[dict] = []
            prior_hashes: dict[str, str] = {}
            if prior_run_id:
                for r in conn.execute(
                        "SELECT source_id,row_hash FROM sig_run_cases "
                        "WHERE run_id=?", (prior_run_id,)):
                    prior_hashes[r["source_id"]] = r["row_hash"]
            for s in sources:
                sid = s["id"]
                ver = SourceService.visible_version(conn, sid, cutoff)
                if ver is None:
                    continue  # 截止线前无任何版本
                if ver["change_type"] == "WITHDRAWAL":
                    continue  # 截止线前已退出样本
                if not (window_start <= ver["event_date"] < window_end):
                    continue
                link = SourceService.as_of_case(conn, sid, cutoff)
                if link is None:
                    raise RunnerError(
                        "ORPHAN_SOURCE",
                        f"来源 {sid} 在截止线无有效病例归属")
                frozen.append({
                    "source_id": sid, "version_id": ver["id"],
                    "version_no": ver["version_no"], "case_id": link["case_id"],
                    "event_date": ver["event_date"],
                    "prior_row_hash": prior_hashes.get(sid),
                    # 新来源或指纹缺失（基线窗口不在样本内）必脏；
                    # 其余先标干净，构建时以实际 row_hash 再判定。
                    "dirty": 0 if sid in prior_hashes else 1,
                })

            shard_count = max(1, (len(frozen) + shard_size - 1) // shard_size)
            job_id = _uid()
            now = _now(self.clock)
            spec = DefinitionService.load_spec(conn, definition_version_id)
            manifest = input_manifest(
                definition_spec=spec, term_catalog_id=term_catalog_id,
                window_start=window_start, window_end=window_end, cutoff=cutoff,
                mode=mode, frozen=frozen)
            input_hash = _sha(_dumps(manifest))
            conn.execute(
                "INSERT INTO sig_jobs(id,computation_key,definition_id,"
                "definition_version_id,definition_version_no,term_catalog_id,"
                "window_start,window_end,cutoff,mode,prior_run_id,shard_count,"
                "status,total_sources,input_hash,requested_by,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'PENDING',?,?,?,?)",
                (job_id, key, definition_id, definition_version_id, vno,
                 term_catalog_id, window_start, window_end, cutoff, mode,
                 prior_run_id, shard_count, len(frozen), input_hash,
                 requested_by, now))
            for shard_no in range(shard_count):
                conn.execute(
                    "INSERT INTO sig_job_shards(job_id,shard_no,status) "
                    "VALUES(?,?,'PENDING')", (job_id, shard_no))
            for i, f in enumerate(sorted(frozen, key=lambda x: x["source_id"])):
                shard_no = i // shard_size
                conn.execute(
                    "INSERT INTO sig_job_sources(job_id,shard_no,source_id,"
                    "version_id,version_no,case_id,event_date,prior_row_hash,dirty) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (job_id, shard_no, f["source_id"], f["version_id"],
                     f["version_no"], f["case_id"], f["event_date"],
                     f["prior_row_hash"], f["dirty"]))
            audit.append_event(
                conn, "RUN_SUBMITTED",
                f"提交窗口作业 {definition_id} [{window_start},{window_end}) "
                f"cutoff={cutoff} {mode}",
                payload={"job_id": job_id, "computation_key": key,
                         "mode": mode, "sources": len(frozen),
                         "shards": shard_count, "input_hash": input_hash,
                         "prior_run_id": prior_run_id},
                actor=requested_by, when=now,
                idempotency_key=f"submit:{key}")
            return self.get_job(job_id, conn=conn)

    # ------------------------------------------------------------------
    # 分片执行
    # ------------------------------------------------------------------
    def run_shard(self, job_id: str, shard_no: int) -> dict:
        """执行单个分片（短事务；已提交则幂等跳过）。"""
        with self._lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    shard = conn.execute(
                        "SELECT * FROM sig_job_shards WHERE job_id=? AND shard_no=?",
                        (job_id, shard_no)).fetchone()
                    if not shard:
                        raise RunnerError("SHARD_NOT_FOUND",
                                          f"分片不存在: {job_id}#{shard_no}")
                    if shard["status"] == "COMMITTED":
                        return {"job_id": job_id, "shard_no": shard_no,
                                "deduped": True}
                    job = conn.execute("SELECT * FROM sig_jobs WHERE id=?",
                                       (job_id,)).fetchone()
                    if job["status"] == "COMPLETED":
                        return {"job_id": job_id, "shard_no": shard_no,
                                "deduped": True, "completed": True}
                    if job["status"] != "FAILED":
                        conn.execute("UPDATE sig_jobs SET status='RUNNING', "
                                     "started_at=COALESCE(started_at,?) WHERE id=?",
                                     (_now(self.clock), job_id))
                    spec = DefinitionService.load_spec(
                        conn, job["definition_version_id"])
                    catalog = TermCatalog.load(conn, job["term_catalog_id"])
                    frozen = conn.execute(
                        "SELECT * FROM sig_job_sources WHERE job_id=? AND shard_no=? "
                        "ORDER BY source_id", (job_id, shard_no)).fetchall()
                    staged = 0
                    for f in frozen:
                        # 增量模式：基线存在且构建后指纹相同 -> 直接复制旧行，
                        # 不重复计算（分片层增量；dirty 以真实指纹为准）。
                        payload = json.loads(conn.execute(
                            "SELECT payload_json FROM sig_source_versions WHERE id=?",
                            (f["version_id"],)).fetchone()["payload_json"])
                        row = build_source_row(
                            source_id=f["source_id"], version_id=f["version_id"],
                            version_no=f["version_no"], case_id=f["case_id"],
                            event_date=f["event_date"], payload=payload,
                            catalog=catalog, spec=spec)
                        is_dirty = (f["prior_row_hash"] is None
                                    or f["prior_row_hash"] != row["row_hash"])
                        if (job["mode"] == "INCREMENTAL" and not is_dirty
                                and f["prior_row_hash"] is not None):
                            old = conn.execute(
                                "SELECT * FROM sig_run_cases WHERE run_id=? "
                                "AND source_id=?",
                                (job["prior_run_id"], f["source_id"])).fetchone()
                            self._insert_row(conn, job_id, shard_no, row, old=old)
                        else:
                            self._insert_row(conn, job_id, shard_no, row)
                        staged += 1
                    conn.execute(
                        "UPDATE sig_job_shards SET status='COMMITTED',source_count=?,"
                        "committed_at=? WHERE job_id=? AND shard_no=?",
                        (staged, _now(self.clock), job_id, shard_no))
                    if self.crash_hook:
                        self.crash_hook("shard_committed", job_id, shard_no)
                if self.crash_hook:
                    # 事务已提交后再触发：模拟进程此刻被杀（分片 COMMITTED、
                    # 作业停在 RUNNING，重启 recover 后从下一分片继续）。
                    self.crash_hook("after_shard_commit", job_id, shard_no)
                return {"job_id": job_id, "shard_no": shard_no,
                        "staged": staged, "deduped": False}
            except RunnerError:
                raise
            except Exception as exc:  # 分片失败：标记 FAILED，但绝不发布
                self._mark_failed(job_id, shard_no, "SHARD_ERROR", str(exc))
                raise

    @staticmethod
    def _insert_row(conn, job_id, shard_no, row, *, old=None) -> None:
        """staging 行按 (job,source) 唯一；续跑重放命中即跳过。

        增量模式的干净分片直接从上一已发布窗口逐字段复制（同一指纹必然
        同一行，避免重复解析，也体现“增量只处理变化来源”）。
        """
        if old is not None:
            conn.execute(
                "INSERT OR IGNORE INTO sig_job_case_rows(job_id,shard_no,source_id,"
                "version_id,version_no,case_id,event_date,drug_group_ids_json,"
                "event_group_ids_json,drug_codes_json,event_codes_json,"
                "causality_levels_json,included,exclude_reason,row_hash) "
                "SELECT ?,?,source_id,version_id,version_no,case_id,event_date,"
                "drug_group_ids_json,event_group_ids_json,drug_codes_json,"
                "event_codes_json,causality_levels_json,included,exclude_reason,"
                "row_hash FROM sig_run_cases WHERE run_id=? AND source_id=?",
                (job_id, shard_no, old["run_id"], old["source_id"]))
            return
        conn.execute(
            "INSERT OR IGNORE INTO sig_job_case_rows(job_id,shard_no,source_id,"
            "version_id,version_no,case_id,event_date,drug_group_ids_json,"
            "event_group_ids_json,drug_codes_json,event_codes_json,"
            "causality_levels_json,included,exclude_reason,row_hash) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (job_id, shard_no, row["source_id"], row["version_id"],
             row["version_no"], row["case_id"], row["event_date"],
             _dumps(row["drug_group_ids"]),
             _dumps(row["event_group_ids"]), _dumps(row["drug_codes"]),
             _dumps(row["event_codes"]),
             _dumps(row["causality_levels"]),
             row["included"], row["exclude_reason"], row["row_hash"]))

    def _mark_failed(self, job_id: str, shard_no: int | None, code: str,
                     message: str) -> None:
        try:
            with transaction(self._conn()) as conn:
                if shard_no is not None:
                    conn.execute(
                        "UPDATE sig_job_shards SET status='FAILED',error_code=?,"
                        "error_message=? WHERE job_id=? AND shard_no=? AND status!='COMMITTED'",
                        (code, message, job_id, shard_no))
                conn.execute(
                    "UPDATE sig_jobs SET status='FAILED',error_code=?,error_message=?,"
                    "finished_at=? WHERE id=? AND status!='COMPLETED'",
                    (code, message, _now(self.clock), job_id))
                audit.append_event(
                    conn, "RUN_FAILED", f"作业失败 {job_id}: {code}",
                    payload={"job_id": job_id, "shard_no": shard_no,
                             "error_code": code, "error_message": message},
                    when=_now(self.clock), idempotency_key=None)
        except Exception:
            pass  # 标记失败本身尽力而为（原始异常继续上抛）

    # ------------------------------------------------------------------
    # 发布（单事务原子提交）
    # ------------------------------------------------------------------
    def finalize(self, job_id: str) -> dict:
        """全部分片提交后原子发布窗口与信号修订。"""
        with self._lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    job = conn.execute("SELECT * FROM sig_jobs WHERE id=?",
                                       (job_id,)).fetchone()
                    if not job:
                        raise RunnerError("JOB_NOT_FOUND", f"作业不存在: {job_id}")
                    if job["status"] == "COMPLETED":
                        return {"job_id": job_id, "run_id": job["run_id"],
                                "deduped": True}
                    shards = conn.execute(
                        "SELECT status,COUNT(*) AS n FROM sig_job_shards "
                        "WHERE job_id=? GROUP BY status", (job_id,)).fetchall()
                    by_status = {r["status"]: r["n"] for r in shards}
                    if by_status.get("FAILED"):
                        raise RunnerError("SHARD_FAILED",
                                          "存在失败分片，不发布半成品指标")
                    if by_status.get("PENDING"):
                        raise RunnerError("SHARDS_PENDING",
                                          f"尚有 {by_status['PENDING']} 个分片未提交")
                    rows = [dict(r) for r in conn.execute(
                        "SELECT * FROM sig_job_case_rows WHERE job_id=? "
                        "ORDER BY source_id", (job_id,))]
                    parsed = [self._row_from_db(r) for r in rows]
                    spec = DefinitionService.load_spec(
                        conn, job["definition_version_id"])
                    pairs = aggregate_pairs(parsed, spec)

                    run_id = _uid()
                    now = _now(self.clock)
                    output = {
                        "cases": sorted(
                            ({"source_id": r["source_id"],
                              "version_id": r["version_id"],
                              "version_no": r["version_no"],
                              "case_id": r["case_id"],
                              "event_date": r["event_date"],
                              "drug_group_ids": r["drug_group_ids"],
                              "event_group_ids": r["event_group_ids"],
                              "drug_codes": r["drug_codes"],
                              "event_codes": r["event_codes"],
                              "causality_levels": r["causality_levels"],
                              "included": r["included"],
                              "exclude_reason": r["exclude_reason"],
                              "row_hash": r["row_hash"]} for r in parsed),
                            key=lambda x: x["source_id"]),
                        "pairs": sorted(
                            ({"drug_group_id": p["drug_group_id"],
                              "event_group_id": p["event_group_id"],
                              **_pair_content(p),
                              "a_source_ids": p["a_source_ids"]}
                             for p in pairs),
                            key=lambda p: (p["drug_group_id"],
                                           p["event_group_id"])),
                    }
                    output_hash = _sha(_dumps(output))
                    included_sources = sum(1 for r in parsed if r["included"] == 1)
                    manifest = {
                        "job_id": job_id, "computation_key": job["computation_key"],
                        "input_hash": job["input_hash"], "output_hash": output_hash,
                        "mode": job["mode"], "total_sources": len(parsed),
                        "included_sources": included_sources,
                        "definition_version_id": job["definition_version_id"],
                        "term_catalog_id": job["term_catalog_id"],
                        "window": {"start": job["window_start"],
                                   "end": job["window_end"], "cutoff": job["cutoff"]},
                    }
                    conn.execute(
                        "INSERT INTO sig_runs(id,job_id,computation_key,definition_id,"
                        "definition_version_id,definition_version_no,term_catalog_id,"
                        "window_start,window_end,cutoff,mode,prior_run_id,"
                        "total_sources,included_sources,input_hash,output_hash,"
                        "manifest_json,published_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (run_id, job_id, job["computation_key"], job["definition_id"],
                         job["definition_version_id"], job["definition_version_no"],
                         job["term_catalog_id"], job["window_start"], job["window_end"],
                         job["cutoff"], job["mode"], job["prior_run_id"],
                         len(parsed), included_sources, job["input_hash"],
                         output_hash, _dumps(manifest), now))
                    for r in parsed:
                        conn.execute(
                            "INSERT INTO sig_run_cases(run_id,source_id,version_id,"
                            "version_no,case_id,event_date,drug_group_ids_json,"
                            "event_group_ids_json,drug_codes_json,event_codes_json,"
                            "causality_levels_json,included,exclude_reason,row_hash) "
                            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (run_id, r["source_id"], r["version_id"], r["version_no"],
                             r["case_id"], r["event_date"],
                             _dumps(r["drug_group_ids"]), _dumps(r["event_group_ids"]),
                             _dumps(r["drug_codes"]), _dumps(r["event_codes"]),
                             _dumps(r["causality_levels"]), r["included"],
                             r["exclude_reason"], r["row_hash"]))
                    revision_ids = self._publish_revisions(conn, run_id, pairs, now)
                    for p in pairs:
                        conn.execute(
                            "INSERT INTO sig_run_pair_metrics(run_id,drug_group_id,"
                            "event_group_id,a,b,c,d,n,metrics_json,decision_json,"
                            "a_source_ids_json,content_hash,revision_id) "
                            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (run_id, p["drug_group_id"], p["event_group_id"],
                             p["a"], p["b"], p["c"], p["d"], p["n"],
                             _dumps(p["counts"]), _dumps(p["decision"]),
                             _dumps(p["a_source_ids"]), _pair_hash(p),
                             revision_ids[(p["drug_group_id"], p["event_group_id"])]))
                    conn.execute(
                        "UPDATE sig_jobs SET status='COMPLETED',run_id=?,"
                        "output_hash=?,finished_at=? WHERE id=?",
                        (run_id, output_hash, now, job_id))
                    audit.append_event(
                        conn, "RUN_PUBLISHED",
                        f"发布窗口 {run_id}（{job['mode']}，来源 {len(parsed)}，"
                        f"信号修订 {sum(1 for v in revision_ids.values() if v)}）",
                        payload={"job_id": job_id, "run_id": run_id,
                                 "computation_key": job["computation_key"],
                                 "output_hash": output_hash,
                                 "new_revisions": [v for v in revision_ids.values()
                                                   if v],
                                 "mode": job["mode"]},
                        when=now, idempotency_key=f"publish:{job_id}")
                if self.crash_hook:
                    self.crash_hook("after_publish", job_id, None)
                return {"job_id": job_id, "run_id": run_id,
                        "output_hash": output_hash}
            except RunnerError:
                raise
            except Exception as exc:
                self._mark_failed(job_id, None, "FINALIZE_ERROR", str(exc))
                raise

    def _publish_revisions(self, conn, run_id: str, pairs: list[dict],
                           now: str) -> dict[tuple[str, str], str | None]:
        """为内容变化的组对追加信号修订；未变化复用上一修订（保留处置）。"""
        job = conn.execute("SELECT * FROM sig_jobs WHERE id=(SELECT job_id "
                           "FROM sig_runs WHERE id=?)", (run_id,)).fetchone()
        definition_id = job["definition_id"]
        prior_pairs: dict[tuple[str, str], dict] = {}
        if job["prior_run_id"]:
            for r in conn.execute(
                    "SELECT * FROM sig_run_pair_metrics WHERE run_id=?",
                    (job["prior_run_id"],)):
                prior_pairs[(r["drug_group_id"], r["event_group_id"])] = dict(r)
        # 基线也可能是更早窗口（增量基线）；修订链按 signal_key 追溯最新。
        out: dict[tuple[str, str], str | None] = {}
        for p in pairs:
            key = (p["drug_group_id"], p["event_group_id"])
            signal_key = f"{definition_id}|{p['drug_group_id']}|{p['event_group_id']}"
            latest = conn.execute(
                "SELECT * FROM sig_signal_revisions WHERE signal_key=? "
                "ORDER BY seq_no DESC LIMIT 1", (signal_key,)).fetchone()
            content = _pair_content(p)
            content_h = _pair_hash(p)
            if latest is not None:
                latest_content = {
                    "a": json.loads(latest["counts_json"])["a"],
                    "b": json.loads(latest["counts_json"])["b"],
                    "c": json.loads(latest["counts_json"])["c"],
                    "d": json.loads(latest["counts_json"])["d"],
                    "n": json.loads(latest["counts_json"])["n"],
                    "metrics": {k: v for k, v in
                                json.loads(latest["metrics_json"]).items()
                                if k in ("prr", "prr_ci95_lower", "prr_ci95_upper",
                                         "ror", "ror_ci95_lower",
                                         "ror_ci95_upper", "chi_square")},
                    "decision": json.loads(latest["decision_json"])["decision"],
                }
                same = (_dumps(latest_content) == _dumps(content))
                if same:
                    out[key] = latest["id"]  # 复用：旧处置保留，不产生新修订
                    continue
                # 撤销：上一修订是 SIGNAL，本次不再构成信号（a 格清零或
                # 跌出阈值）。判定基于修订链本身，不依赖本次是否增量。
                latest_was_signal = json.loads(
                    latest["decision_json"]).get("is_signal", False)
                change_type = "WITHDRAWN" if (
                    latest_was_signal and not content["decision"]
                    == "SIGNAL") else "COUNT_CHANGE"
                rev_id = _uid()
                conn.execute(
                    "INSERT INTO sig_signal_revisions(id,signal_key,definition_id,"
                    "drug_group_id,event_group_id,seq_no,run_id,prev_revision_id,"
                    "change_type,counts_json,metrics_json,decision_json,content_hash,"
                    "created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (rev_id, signal_key, definition_id, p["drug_group_id"],
                     p["event_group_id"], latest["seq_no"] + 1, run_id, latest["id"],
                     change_type, _dumps({"a": p["a"], "b": p["b"], "c": p["c"],
                                          "d": p["d"], "n": p["n"]}),
                     _dumps(p["counts"]), _dumps(p["decision"]), content_h, now))
                out[key] = rev_id
            else:
                # 全新组对：只有实际构成信号（a>0 且判定 SIGNAL）才起修订链。
                if p["a"] > 0 and p["decision"]["is_signal"]:
                    rev_id = _uid()
                    conn.execute(
                        "INSERT INTO sig_signal_revisions(id,signal_key,definition_id,"
                        "drug_group_id,event_group_id,seq_no,run_id,prev_revision_id,"
                        "change_type,counts_json,metrics_json,decision_json,"
                        "content_hash,created_at) VALUES(?,?,?,?,?,?,?,?,"
                        "'NEW',?,?,?,?,?)",
                        (rev_id, signal_key, definition_id, p["drug_group_id"],
                         p["event_group_id"], 1, run_id, None,
                         _dumps({"a": p["a"], "b": p["b"], "c": p["c"],
                                 "d": p["d"], "n": p["n"]}),
                         _dumps(p["counts"]), _dumps(p["decision"]), content_h, now))
                    out[key] = rev_id
                else:
                    out[key] = None
        return out

    @staticmethod
    def _row_from_db(r) -> dict:
        return {
            "source_id": r["source_id"], "version_id": r["version_id"],
            "version_no": r["version_no"], "case_id": r["case_id"],
            "event_date": r["event_date"],
            "drug_group_ids": json.loads(r["drug_group_ids_json"]),
            "event_group_ids": json.loads(r["event_group_ids_json"]),
            "drug_codes": json.loads(r["drug_codes_json"]),
            "event_codes": json.loads(r["event_codes_json"]),
            "causality_levels": json.loads(r["causality_levels_json"]),
            "included": r["included"], "exclude_reason": r["exclude_reason"],
            "row_hash": r["row_hash"],
        }

    # ------------------------------------------------------------------
    # 推进 / 恢复 / 查询
    # ------------------------------------------------------------------
    def tick(self, job_id: str) -> dict:
        """推进一个分片；全部分片完成则自动发布。返回动作描述。"""
        with self._lock:
            conn = self._conn()
            try:
                job = conn.execute("SELECT status FROM sig_jobs WHERE id=?",
                                   (job_id,)).fetchone()
                if not job:
                    raise RunnerError("JOB_NOT_FOUND", f"作业不存在: {job_id}")
                if job["status"] == "COMPLETED":
                    return {"job_id": job_id, "action": "COMPLETED"}
                shard = conn.execute(
                    "SELECT shard_no FROM sig_job_shards WHERE job_id=? "
                    "AND status='PENDING' ORDER BY shard_no LIMIT 1",
                    (job_id,)).fetchone()
                failed = conn.execute(
                    "SELECT shard_no FROM sig_job_shards WHERE job_id=? "
                    "AND status='FAILED' ORDER BY shard_no LIMIT 1",
                    (job_id,)).fetchone()
            finally:
                conn.close()
            if failed:
                # FAILED 分片必须经 recover/重试重新置 PENDING 才可续跑。
                raise RunnerError("SHARD_FAILED",
                                  f"分片 {failed['shard_no']} 已失败，请先恢复")
            if shard is None:
                return self.finalize(job_id)
            self.run_shard(job_id, shard["shard_no"])
            return {"job_id": job_id, "action": "SHARD_COMMITTED",
                    "shard_no": shard["shard_no"]}

    def run_to_completion(self, job_id: str, *, max_shards: int | None = None) -> dict:
        """同步跑完（默认无后台线程时的便捷方法）。"""
        done = 0
        while True:
            r = self.tick(job_id)
            if r.get("action") == "COMPLETED" or "run_id" in r:
                return r
            done += 1
            if max_shards is not None and done >= max_shards:
                return r

    def recover(self) -> dict:
        """崩溃恢复：中断（RUNNING）的作业重置为可续跑，失败分片复位。

        已 COMMITTED 分片与 staging 行不动（检查点），只补未完成部分；
        重复恢复不产生重复审计事件（幂等键按作业+复位分片集合）。

        两种崩溃形态都覆盖：

        - 分片事务失败（Python 异常）：分片 FAILED + 作业 FAILED，删除该
          分片的暂存行并复位 PENDING 重做；
        - 进程在分片 COMMITTED 后、作业完成前死亡：分片 COMMITTED 但
          作业停在 RUNNING（行已落盘），作业复位 PENDING 后 tick 跳过已
          提交分片，绝不重做、不重复贡献。
        """
        with self._lock, transaction(self._conn()) as conn:
            running = [r["id"] for r in conn.execute(
                "SELECT id FROM sig_jobs WHERE status='RUNNING'")]
            for jid in running:
                conn.execute("UPDATE sig_jobs SET status='PENDING' WHERE id=?",
                             (jid,))
            reset_shards = 0
            reset_job_ids: list[str] = []
            for r in conn.execute(
                    "SELECT job_id,shard_no FROM sig_job_shards "
                    "WHERE status='FAILED'"):
                # 失败分片可能已写部分 staging 行；删除该分片未完成行，
                # 由唯一约束 + 重算保证幂等（COMMITTED 分片行不动）。
                conn.execute(
                    "DELETE FROM sig_job_case_rows WHERE job_id=? AND shard_no=?",
                    (r["job_id"], r["shard_no"]))
                conn.execute(
                    "UPDATE sig_job_shards SET status='PENDING',error_code=NULL,"
                    "error_message=NULL,source_count=0,committed_at=NULL "
                    "WHERE job_id=? AND shard_no=?",
                    (r["job_id"], r["shard_no"]))
                reset_shards += 1
                reset_job_ids.append(r["job_id"])
            # 作业因分片异常 FAILED 且现在已无失败分片（全部复位）-> PENDING；
            # 若仍有 FAILED 分片，保持 FAILED，由 tick 明确拒绝、等待恢复。
            for r in conn.execute(
                    "SELECT j.id FROM sig_jobs j WHERE j.status='FAILED' AND NOT "
                    "EXISTS (SELECT 1 FROM sig_job_shards s WHERE s.job_id=j.id "
                    "AND s.status='FAILED')"):
                conn.execute(
                    "UPDATE sig_jobs SET status='PENDING',error_code=NULL,"
                    "error_message=NULL WHERE id=?", (r["id"],))
                reset_job_ids.append(r["id"])
            all_jobs = sorted(set(running) | set(reset_job_ids))
            # 无实质动作的恢复调用不落审计（重复 recover 完全静默）。
            if all_jobs or reset_shards:
                audit.append_event(
                    conn, "RUN_RECOVERED",
                    f"恢复 {len(all_jobs)} 个中断作业，"
                    f"复位 {reset_shards} 个失败分片",
                    payload={"reset_jobs": all_jobs,
                             "reset_shards": reset_shards},
                    when=_now(self.clock),
                    idempotency_key=(f"recover:{_sha(_dumps(all_jobs))}:"
                                     f"{reset_shards}"))
            return {"reset_jobs": all_jobs, "reset_shards": reset_shards}

    def get_job(self, job_id: str, *, conn=None) -> dict:
        own = conn is None
        conn = conn or self._conn()
        try:
            j = conn.execute("SELECT * FROM sig_jobs WHERE id=?",
                             (job_id,)).fetchone()
            if not j:
                raise RunnerError("JOB_NOT_FOUND", f"作业不存在: {job_id}")
            d = dict(j)
            d["shards"] = [dict(s) for s in conn.execute(
                "SELECT shard_no,status,source_count,error_code,committed_at "
                "FROM sig_job_shards WHERE job_id=? ORDER BY shard_no", (job_id,))]
            committed = sum(1 for s in d["shards"] if s["status"] == "COMMITTED")
            d["committed_shards"] = committed
            return d
        finally:
            if own:
                conn.close()

    def list_jobs(self, *, status: str | None = None,
                  definition_id: str | None = None) -> list[dict]:
        conn = self._conn()
        try:
            where, params = [], []
            if status:
                where.append("status=?")
                params.append(status)
            if definition_id:
                where.append("definition_id=?")
                params.append(definition_id)
            sql = ("SELECT id,computation_key,definition_id,definition_version_no,"
                   "term_catalog_id,window_start,window_end,cutoff,mode,status,"
                   "run_id,shard_count,total_sources,input_hash,output_hash,"
                   "error_code,created_at,started_at,finished_at FROM sig_jobs")
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY created_at DESC"
            return [dict(r) for r in conn.execute(sql, params)]
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 重新计算核验（不写库）：冻结输入 -> 纯函数重算 -> 对比已发布 output_hash
    # ------------------------------------------------------------------
    def verify_run(self, run_id: str) -> dict:
        """按已发布 run 的冻结输入独立重算，逐来源/逐组对比对。"""
        conn = self._conn()
        try:
            run = conn.execute("SELECT * FROM sig_runs WHERE id=?",
                               (run_id,)).fetchone()
            if not run:
                raise RunnerError("RUN_NOT_FOUND", f"窗口不存在: {run_id}")
            job = conn.execute("SELECT * FROM sig_jobs WHERE id=?",
                               (run["job_id"],)).fetchone()
            spec = DefinitionService.load_spec(conn, run["definition_version_id"])
            catalog = TermCatalog.load(conn, run["term_catalog_id"])
            frozen = conn.execute(
                "SELECT * FROM sig_job_sources WHERE job_id=? ORDER BY source_id",
                (job["id"],)).fetchall()
            mismatches: list[dict] = []
            rebuilt_rows = []
            for f in frozen:
                payload = json.loads(conn.execute(
                    "SELECT payload_json FROM sig_source_versions WHERE id=?",
                    (f["version_id"],)).fetchone()["payload_json"])
                row = build_source_row(
                    source_id=f["source_id"], version_id=f["version_id"],
                    version_no=f["version_no"], case_id=f["case_id"],
                    event_date=f["event_date"], payload=payload,
                    catalog=catalog, spec=spec)
                pub = conn.execute(
                    "SELECT row_hash FROM sig_run_cases WHERE run_id=? AND source_id=?",
                    (run_id, f["source_id"])).fetchone()
                if not pub or pub["row_hash"] != row["row_hash"]:
                    mismatches.append({"kind": "CASE_ROW",
                                       "source_id": f["source_id"],
                                       "expected": pub["row_hash"] if pub else None,
                                       "actual": row["row_hash"]})
                rebuilt_rows.append(row)
            pairs = aggregate_pairs(rebuilt_rows, spec)
            for p in pairs:
                pub = conn.execute(
                    "SELECT content_hash FROM sig_run_pair_metrics WHERE run_id=? "
                    "AND drug_group_id=? AND event_group_id=?",
                    (run_id, p["drug_group_id"], p["event_group_id"])).fetchone()
                if not pub or pub["content_hash"] != _pair_hash(p):
                    mismatches.append({"kind": "PAIR",
                                       "drug_group_id": p["drug_group_id"],
                                       "event_group_id": p["event_group_id"]})
            output = {
                "cases": sorted(
                    ({"source_id": r["source_id"], "version_id": r["version_id"],
                      "version_no": r["version_no"], "case_id": r["case_id"],
                      "event_date": r["event_date"],
                      "drug_group_ids": r["drug_group_ids"],
                      "event_group_ids": r["event_group_ids"],
                      "drug_codes": r["drug_codes"],
                      "event_codes": r["event_codes"],
                      "causality_levels": r["causality_levels"],
                      "included": r["included"], "exclude_reason": r["exclude_reason"],
                      "row_hash": r["row_hash"]} for r in rebuilt_rows),
                    key=lambda x: x["source_id"]),
                "pairs": sorted(
                    ({"drug_group_id": p["drug_group_id"],
                      "event_group_id": p["event_group_id"], **_pair_content(p),
                      "a_source_ids": p["a_source_ids"]} for p in pairs),
                    key=lambda p: (p["drug_group_id"], p["event_group_id"])),
            }
            recomputed_hash = _sha(_dumps(output))
            return {
                "run_id": run_id, "valid": not mismatches
                and recomputed_hash == run["output_hash"],
                "stored_output_hash": run["output_hash"],
                "recomputed_output_hash": recomputed_hash,
                "hash_match": recomputed_hash == run["output_hash"],
                "mismatches": mismatches,
            }
        finally:
            conn.close()
