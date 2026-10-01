"""病例来源服务：版本接收（迟到随访/退出样本）、归并拆解、as-of 解析。

关键规则：

- 来源是四格计数与贡献追踪的最小单位；来源版本 ``sig_source_versions``
  只增，迟到随访/因果评价修正产生新版本（不覆盖），作业以
  ``recorded_at < cutoff`` 取截止线可见版本。
- ``WITHDRAWAL`` 版本表示来源退出后续窗口样本（撤销其统计贡献由后续
  窗口的差异追踪体现，历史窗口快照永不修改）。
- 来源→病例归属 ``sig_case_links`` 只增（旧行 1→0 + 新行 + 血缘闭包），
  部分唯一索引保证一个来源任何时刻只在一个当前病例里。
- ``freeze_sources`` 在作业提交事务内枚举窗口候选来源并冻结
  （as-of 版本 + as-of 归属 + 分片号 + 增量脏标记），分片执行只读冻结集。
"""
from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from ..service import Clock
from . import audit
from .db import transaction


def _now(clock: Clock) -> str:
    return clock.now().isoformat(timespec="seconds")


def _uid() -> str:
    return uuid.uuid4().hex


def payload_hash(payload: dict) -> str:
    canon = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), default=str)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


class SourceError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def _validate_payload(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise SourceError("BAD_PAYLOAD", "payload 必须是对象")
    drugs = payload.get("drugs")
    events = payload.get("events")
    if drugs is not None and not isinstance(drugs, list):
        raise SourceError("BAD_PAYLOAD", "drugs 必须是数组")
    if events is not None and not isinstance(events, list):
        raise SourceError("BAD_PAYLOAD", "events 必须是数组")


class SourceService:
    def __init__(self, db_path: str, clock: Clock):
        self.db_path = db_path
        self.clock = clock

    def _conn(self):
        from .db import connect
        return connect(self.db_path)

    # ------------------------------------------------------------------
    # 接收 / 随访 / 退出
    # ------------------------------------------------------------------
    def intake(self, source_id: str, origin: str, payload: dict, *,
               event_date: str | None = None, actor: str | None = None,
               recorded_at: str | None = None,
               idempotency_key: str | None = None) -> dict:
        """接收首版来源（同 origin+external_id 幂等由 source_id 显式控制）。"""
        _validate_payload(payload)
        event_date = event_date or payload.get("event_date")
        if not event_date:
            raise SourceError("NO_EVENT_DATE", "首版必须提供 event_date")
        now = _now(self.clock)
        recorded_at = recorded_at or now
        with transaction(self._conn()) as conn:
            if conn.execute("SELECT 1 FROM sig_sources WHERE id=?",
                            (source_id,)).fetchone():
                raise SourceError("SOURCE_EXISTS", f"来源已存在: {source_id}")
            case_id = _uid()
            conn.execute(
                "INSERT INTO sig_cases(id,status,created_at) VALUES(?,?,?)",
                (case_id, "OPEN", now))
            conn.execute(
                "INSERT INTO sig_sources(id,origin,external_id,active,withdrawn,"
                "created_at,updated_at) VALUES(?,?,?,1,0,?,?)",
                (source_id, origin, source_id, now, now))
            version_id = _uid()
            h = payload_hash(payload)
            conn.execute(
                "INSERT INTO sig_source_versions(id,source_id,version_no,change_type,"
                "event_date,payload_json,content_hash,idempotency_key,recorded_at,"
                "created_at) VALUES(?,?,1,'INTAKE',?,?,?,?,?,?)",
                (version_id, source_id, str(event_date),
                 json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str),
                 h, idempotency_key, recorded_at, now))
            conn.execute(
                "INSERT INTO sig_case_links(id,source_id,case_id,active,reason,"
                "effective_at,actor,created_at) VALUES(?,?,?,1,'INTAKE',?,?,?)",
                (_uid(), source_id, case_id, recorded_at, actor, now))
            conn.execute(
                "INSERT INTO sig_case_lineage(ancestor_id,descendant_id,depth) "
                "VALUES(?,? ,0)", (case_id, case_id))
            audit.append_event(
                conn, "SOURCE_INTAKE", f"接收来源 {source_id}",
                payload={"source_id": source_id, "case_id": case_id,
                         "version_id": version_id, "event_date": event_date},
                actor=actor, when=now,
                idempotency_key=f"src-intake:{source_id}")
            return {"source_id": source_id, "case_id": case_id,
                    "version_id": version_id, "version_no": 1}

    def add_version(self, source_id: str, payload: dict, *,
                    change_type: str = "FOLLOWUP",
                    event_date: str | None = None,
                    actor: str | None = None, recorded_at: str | None = None,
                    idempotency_key: str | None = None) -> dict:
        """追加版本：FOLLOWUP（迟到随访/因果评价修正）或 WITHDRAWAL（退出样本）。

        携带相同 idempotency_key 的重放返回既有版本。
        """
        if change_type not in ("FOLLOWUP", "WITHDRAWAL"):
            raise SourceError("BAD_CHANGE_TYPE",
                              "change_type 必须是 FOLLOWUP 或 WITHDRAWAL")
        _validate_payload(payload)
        now = _now(self.clock)
        recorded_at = recorded_at or now
        with transaction(self._conn()) as conn:
            src = conn.execute(
                "SELECT id,active FROM sig_sources WHERE id=?",
                (source_id,)).fetchone()
            if not src:
                raise SourceError("SOURCE_NOT_FOUND", f"来源不存在: {source_id}")
            if idempotency_key:
                old = conn.execute(
                    "SELECT id,version_no FROM sig_source_versions "
                    "WHERE idempotency_key=?", (idempotency_key,)).fetchone()
                if old:
                    return {"source_id": source_id, "version_id": old["id"],
                            "version_no": old["version_no"], "deduped": True}
            prev = conn.execute(
                "SELECT version_no,event_date FROM sig_source_versions "
                "WHERE source_id=? ORDER BY version_no DESC LIMIT 1",
                (source_id,)).fetchone()
            # 退出样本后不再接受新版本；必须先重新启用（新来源）。
            if prev is None:
                raise SourceError("NO_INTAKE", f"来源无首版: {source_id}")
            next_no = prev["version_no"] + 1
            if event_date is None:
                event_date = payload.get("event_date") or prev["event_date"]
            version_id = _uid()
            h = payload_hash(payload)
            conn.execute(
                "INSERT INTO sig_source_versions(id,source_id,version_no,change_type,"
                "event_date,payload_json,content_hash,idempotency_key,recorded_at,"
                "created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (version_id, source_id, next_no, change_type, str(event_date),
                 json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str),
                 h, idempotency_key, recorded_at, now))
            if change_type == "WITHDRAWAL":
                conn.execute("UPDATE sig_sources SET withdrawn=1, updated_at=? "
                             "WHERE id=?", (now, source_id))
            audit.append_event(
                conn,
                "SOURCE_WITHDRAWAL" if change_type == "WITHDRAWAL"
                else "SOURCE_FOLLOWUP",
                f"来源 {source_id} v{next_no} "
                f"{'退出样本' if change_type == 'WITHDRAWAL' else '随访修订'}",
                payload={"source_id": source_id, "version_id": version_id,
                         "version_no": next_no, "event_date": event_date},
                actor=actor, when=now,
                idempotency_key=(f"src-{change_type.lower()}:{source_id}:{next_no}"
                                 if not idempotency_key else idempotency_key))
            return {"source_id": source_id, "version_id": version_id,
                    "version_no": next_no, "deduped": False}

    # ------------------------------------------------------------------
    # 归并 / 拆解
    # ------------------------------------------------------------------
    def merge_cases(self, survivor_case_id: str, other_case_id: str, *,
                    actor: str | None = None, effective_at: str | None = None) -> dict:
        """把 other_case 的全部当前来源逐行归并进 survivor（并查血缘闭包）。

        并发安全在数据库层成立：

        - 每个来源的“停用旧行+插入新行”在自己的 ``BEGIN IMMEDIATE`` 事务内
          先持有 ``sig_case_link_locks`` 行锁，并发转移同一来源只有一个事务
          能成功；归并一批来源不等同于锁住全部来源，并发归并不互相饿死。
        - 病例头 MERGED 状态做 CAS：other 已被别的事务归并时返回 409，
          绝不产生两个存活父病例。
        """
        now = _now(self.clock)
        effective_at = effective_at or now
        moved: list[str] = []
        sids = [r["source_id"] for r in self._conn().execute(
            "SELECT source_id FROM sig_case_links WHERE case_id=? AND active=1 "
            "ORDER BY source_id", (other_case_id,)).fetchall()]
        for sid in sids:
            self._relink_source(sid, survivor_case_id, reason="MERGE",
                                actor=actor, effective_at=effective_at, now=now)
            moved.append(sid)
        with transaction(self._conn()) as conn:
            self._require_case(conn, survivor_case_id)
            cur = conn.execute(
                "SELECT status,superseded_by_id FROM sig_cases WHERE id=?",
                (other_case_id,)).fetchone()
            if not cur:
                raise SourceError("CASE_NOT_FOUND", f"病例不存在: {other_case_id}")
            if cur["status"] == "MERGED" and cur["superseded_by_id"] != survivor_case_id:
                raise SourceError(
                    "CASE_ALREADY_MERGED",
                    f"病例 {other_case_id} 已被归并进其他病例",
                    )
            if survivor_case_id == other_case_id:
                raise SourceError("SAME_CASE", "不能归并病例自身")
            conn.execute(
                "UPDATE sig_cases SET status='MERGED', superseded_by_id=? WHERE id=?",
                (survivor_case_id, other_case_id))
            self._extend_lineage(conn, survivor_case_id, other_case_id)
            audit.append_event(
                conn, "CASE_MERGED",
                f"病例 {other_case_id} 归并进 {survivor_case_id}",
                payload={"survivor": survivor_case_id, "merged": other_case_id,
                         "moved_sources": moved},
                actor=actor, when=now,
                idempotency_key=f"merge:{survivor_case_id}:{other_case_id}:{effective_at}")
        return {"survivor_case_id": survivor_case_id,
                "merged_case_id": other_case_id, "moved_sources": moved}

    def _relink_source(self, source_id: str, new_case_id: str, *, reason: str,
                       actor: str | None, effective_at: str, now: str) -> None:
        """单来源归属转移（独立短事务 + 锁后重读 + CAS）。

        SQLite 写事务在 IMMEDIATE + busy_timeout 下全局排队；但排队期间
        别的事务可能已提交（autocommit 下排队前的 SELECT 快照过时），
        因此**获取写锁后必须重读**当前归属，再以条件 UPDATE 做 CAS：
        并发转移同一来源只有第一个生效，后续在“已经是目标病例/已无旧行”
        时安全收敛或报错。
        """
        with transaction(self._conn()) as conn:
            # 此刻持有全局写锁：读到的是所有前序已提交转移之后的权威状态。
            link = conn.execute(
                "SELECT id,case_id FROM sig_case_links WHERE source_id=? "
                "AND active=1", (source_id,)).fetchone()
            if not link:
                raise SourceError("NO_ACTIVE_LINK",
                                  f"来源无当前归属: {source_id}")
            if link["case_id"] == new_case_id:
                return
            cur = conn.execute(
                "UPDATE sig_case_links SET active=0 WHERE id=? AND active=1",
                (link["id"],))
            if cur.rowcount != 1:
                raise SourceError("MEMBERSHIP_MOVED",
                                  f"来源 {source_id} 归属已被并发操作修改")
            conn.execute(
                "INSERT INTO sig_case_links(id,source_id,case_id,active,reason,"
                "effective_at,actor,created_at) VALUES(?,?,?,1,?,?,?,?)",
                (_uid(), source_id, new_case_id, reason, effective_at, actor, now))

    def split_source(self, source_id: str, *, actor: str | None = None,
                     effective_at: str | None = None,
                     expected_case_id: str | None = None) -> dict:
        """把一个来源从当前病例拆解为独立新病例（持锁后重读 + CAS）。

        ``expected_case_id`` 是乐观条件：调用方（工作台 UI/API）基于读取到
        的当前病例发起拆解；并发的第二个拆解携带同一期望病例时，发现来源
        已不在该病例即 409 ``CASE_MOVED``，保证一次“用户动作”只产生一个
        新病例。缺省时任何当前归属都可拆（供维护/脚本顺序操作）。
        """
        now = _now(self.clock)
        effective_at = effective_at or now
        with transaction(self._conn()) as conn:
            if not conn.execute("SELECT 1 FROM sig_sources WHERE id=?",
                                (source_id,)).fetchone():
                raise SourceError("SOURCE_NOT_FOUND", f"来源不存在: {source_id}")
            # 持写锁后重读权威归属（排队前的快照可能已被前序事务改写）。
            link = conn.execute(
                "SELECT id,case_id FROM sig_case_links WHERE source_id=? AND active=1",
                (source_id,)).fetchone()
            if not link:
                raise SourceError("NO_ACTIVE_LINK",
                                  f"来源无当前归属: {source_id}")
            if expected_case_id and link["case_id"] != expected_case_id:
                raise SourceError(
                    "CASE_MOVED",
                    f"来源 {source_id} 已不在病例 {expected_case_id}（当前 "
                    f"{link['case_id']}），请刷新后重试")
            cur = conn.execute(
                "UPDATE sig_case_links SET active=0 WHERE id=? AND active=1",
                (link["id"],))
            if cur.rowcount != 1:
                raise SourceError("MEMBERSHIP_MOVED",
                                  f"来源 {source_id} 归属已被并发操作修改")
            old_case = link["case_id"]
            new_case = _uid()
            conn.execute(
                "INSERT INTO sig_cases(id,status,created_at) VALUES(?,?,?)",
                (new_case, "OPEN", now))
            conn.execute(
                "INSERT INTO sig_case_links(id,source_id,case_id,active,reason,"
                "effective_at,actor,created_at) VALUES(?,?,?,1,'SPLIT',?,?,?)",
                (_uid(), source_id, new_case, effective_at, actor, now))
            conn.execute(
                "INSERT INTO sig_case_lineage(ancestor_id,descendant_id,depth) "
                "VALUES(?,? ,0)", (new_case, new_case))
            audit.append_event(
                conn, "CASE_SPLIT",
                f"来源 {source_id} 从 {old_case} 拆解为 {new_case}",
                payload={"source_id": source_id, "old_case": old_case,
                         "new_case": new_case},
                actor=actor, when=now,
                idempotency_key=f"split:{source_id}:{effective_at}")
            return {"source_id": source_id, "old_case_id": old_case,
                    "new_case_id": new_case}

    @staticmethod
    def _require_case(conn, case_id: str) -> None:
        r = conn.execute("SELECT id,status FROM sig_cases WHERE id=?",
                         (case_id,)).fetchone()
        if not r:
            raise SourceError("CASE_NOT_FOUND", f"病例不存在: {case_id}")

    @staticmethod
    def _extend_lineage(conn, survivor: str, merged: str) -> None:
        """把 merged 的全部祖先（含自身）接到 survivor 子树下（幂等）。"""
        ancestors = [r["ancestor_id"] for r in conn.execute(
            "SELECT ancestor_id FROM sig_case_lineage WHERE descendant_id=?",
            (merged,))]
        descendants = [r["descendant_id"] for r in conn.execute(
            "SELECT descendant_id FROM sig_case_lineage WHERE ancestor_id=?",
            (survivor,))]
        # (ancestor -> survivor) 深度
        up = {r["ancestor_id"]: r["depth"] for r in conn.execute(
            "SELECT ancestor_id,depth FROM sig_case_lineage WHERE descendant_id=?",
            (survivor,))}
        # (survivor -> descendant) 深度
        down = {r["descendant_id"]: r["depth"] for r in conn.execute(
            "SELECT descendant_id,depth FROM sig_case_lineage WHERE ancestor_id=?",
            (survivor,))}
        for anc in ancestors:
            for des in descendants:
                d = up.get(anc, 1) + down[des]
                # 闭包只增（触发器拒绝 UPDATE）：首次建立的祖先->后代关系即
                # 保留；同对重复合并时 INSERT OR IGNORE 不产生重复边。
                conn.execute(
                    "INSERT OR IGNORE INTO sig_case_lineage"
                    "(ancestor_id,descendant_id,depth) VALUES(?,?,?)",
                    (anc, des, d))

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def list_sources(self) -> list[dict]:
        conn = self._conn()
        try:
            out = []
            for r in conn.execute(
                    "SELECT s.*, sl.case_id FROM sig_sources s "
                    "LEFT JOIN sig_case_links sl ON sl.source_id=s.id AND sl.active=1 "
                    "ORDER BY s.created_at"):
                out.append(dict(r))
            return out
        finally:
            conn.close()

    def get_source(self, source_id: str) -> dict:
        conn = self._conn()
        try:
            r = conn.execute(
                "SELECT s.*, sl.case_id FROM sig_sources s "
                "LEFT JOIN sig_case_links sl ON sl.source_id=s.id AND sl.active=1 "
                "WHERE s.id=?", (source_id,)).fetchone()
            if not r:
                raise SourceError("SOURCE_NOT_FOUND", f"来源不存在: {source_id}")
            d = dict(r)
            d["versions"] = [dict(v) for v in conn.execute(
                "SELECT id,version_no,change_type,event_date,recorded_at,"
                "idempotency_key,created_at FROM sig_source_versions "
                "WHERE source_id=? ORDER BY version_no", (source_id,))]
            return d
        finally:
            conn.close()

    def list_cases(self) -> list[dict]:
        conn = self._conn()
        try:
            out = []
            for r in conn.execute("SELECT * FROM sig_cases ORDER BY created_at"):
                d = dict(r)
                d["source_ids"] = [x["source_id"] for x in conn.execute(
                    "SELECT source_id FROM sig_case_links WHERE case_id=? AND active=1 "
                    "ORDER BY source_id", (r["id"],))]
                out.append(d)
            return out
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # as-of 解析（作业冻结使用，必须在调用方事务内）
    # ------------------------------------------------------------------
    @staticmethod
    def visible_version(conn, source_id: str, cutoff: str):
        """截止线下可见的最新版本（recorded_at < cutoff 严格小于）。"""
        return conn.execute(
            "SELECT * FROM sig_source_versions WHERE source_id=? AND recorded_at < ? "
            "ORDER BY version_no DESC LIMIT 1", (source_id, cutoff)).fetchone()

    @staticmethod
    def as_of_case(conn, source_id: str, cutoff: str):
        """截止线下生效的当前病例（effective_at <= cutoff 的最新归属行）。

        归属行只增不删：转移只是旧行 1→0 + 插入更晚 effective_at 的新行，
        因此按 effective_at 倒序取最近一条即该时刻的 active 归属，无需依赖
        行当前的 active 标志（active 只反映“现在”）。
        """
        return conn.execute(
            "SELECT case_id FROM sig_case_links WHERE source_id=? "
            "AND effective_at <= ? ORDER BY effective_at DESC, created_at DESC LIMIT 1",
            (source_id, cutoff)).fetchone()
