"""信号修订的分析员处置：确认 / 驳回 / 标记观察。

合规规则：

- ``sig_signal_dispositions`` append-only（触发器拒绝改删）：一个修订
  至多一条处置，结论永久保留。
- **旧修订的 CONFIRMED/REJECTED/WATCH 保留原结论**：查询信号时，每个
  修订各自携带自己的处置，历史结论不被新修订覆盖。
- **新修订必须重新处置**：未处置修订的状态为 ``PENDING_TRIAGE``。
- **基于过期修订号的操作被拒绝**：处置时必须携带该 signal_key 当前
  最新 seq_no；旧号返回 409 ``STALE_REVISION``（携带最新号与修订 id）。
"""
from __future__ import annotations

import json
import uuid

from ..service import Clock
from . import audit
from .db import transaction

DECISIONS = ("CONFIRMED", "REJECTED", "WATCH")


def _now(clock: Clock) -> str:
    return clock.now().isoformat(timespec="seconds")


def _uid() -> str:
    return uuid.uuid4().hex


class SignalError(Exception):
    def __init__(self, code: str, message: str, *, extra: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.extra = extra or {}


class SignalService:
    def __init__(self, db_path: str, clock: Clock):
        self.db_path = db_path
        self.clock = clock

    def _conn(self):
        from .db import connect
        return connect(self.db_path)

    def dispose(self, revision_id: str, decision: str, analyst: str, *,
                comment: str | None = None,
                expected_seq_no: int | None = None) -> dict:
        """对信号修订作出确认/驳回/观察结论（乐观修订号 + append-only）。"""
        if decision not in DECISIONS:
            raise SignalError("BAD_DECISION",
                              f"decision 必须是 {DECISIONS} 之一")
        if not analyst or not str(analyst).strip():
            raise SignalError("BLANK_ANALYST", "analyst 不能为空")
        analyst = str(analyst)
        now = _now(self.clock)
        with transaction(self._conn()) as conn:
            rev = conn.execute(
                "SELECT * FROM sig_signal_revisions WHERE id=?",
                (revision_id,)).fetchone()
            if not rev:
                raise SignalError("REVISION_NOT_FOUND",
                                  f"信号修订不存在: {revision_id}")
            latest = conn.execute(
                "SELECT id,seq_no FROM sig_signal_revisions WHERE signal_key=? "
                "ORDER BY seq_no DESC LIMIT 1", (rev["signal_key"],)).fetchone()
            is_latest = latest["id"] == revision_id
            # 过期修订号：无论用 expected_seq_no 还是直接对旧修订操作都拒绝。
            if expected_seq_no is not None and expected_seq_no != latest["seq_no"]:
                raise SignalError(
                    "STALE_REVISION",
                    f"基于过期修订号 v{expected_seq_no} 的操作被拒绝；"
                    f"当前最新 v{latest['seq_no']}",
                    extra={"signal_key": rev["signal_key"],
                           "current_seq_no": latest["seq_no"],
                           "current_revision_id": latest["id"]})
            if not is_latest:
                raise SignalError(
                    "STALE_REVISION",
                    f"修订 v{rev['seq_no']} 已被 v{latest['seq_no']} 取代，"
                    "旧修订结论保留，新修订必须重新处置",
                    extra={"signal_key": rev["signal_key"],
                           "current_seq_no": latest["seq_no"],
                           "current_revision_id": latest["id"]})
            existing = conn.execute(
                "SELECT decision,analyst FROM sig_signal_dispositions "
                "WHERE revision_id=?", (revision_id,)).fetchone()
            if existing:
                raise SignalError(
                    "ALREADY_DISPOSED",
                    f"该修订已由 {existing['analyst']} 处置为 "
                    f"{existing['decision']}（结论不可修改）",
                    extra={"decision": existing["decision"]})
            conn.execute(
                "INSERT INTO sig_signal_dispositions(id,revision_id,signal_key,seq_no,"
                "decision,analyst,comment,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (_uid(), revision_id, rev["signal_key"], rev["seq_no"], decision,
                 analyst, comment, now))
            audit.append_event(
                conn, f"SIGNAL_{decision}",
                f"信号 {rev['signal_key']} v{rev['seq_no']} {decision}",
                payload={"revision_id": revision_id,
                         "signal_key": rev["signal_key"], "seq_no": rev["seq_no"],
                         "decision": decision, "comment": comment},
                actor=analyst, when=now,
                idempotency_key=f"disp:{revision_id}")
            return {"revision_id": revision_id,
                    "signal_key": rev["signal_key"], "seq_no": rev["seq_no"],
                    "decision": decision, "analyst": analyst}

    def list_signals(self, *, definition_id: str | None = None,
                     run_id: str | None = None,
                     status: str | None = None,
                     include_all_revisions: bool = False,
                     limit: int = 500) -> list[dict]:
        """列出信号修订。

        默认每个 signal_key 只返回最新修订（携带处置状态）；
        ``include_all_revisions=True`` 返回全部修订（含已被取代的旧修订
        及其保留的历史结论）。``status`` 按处置状态过滤
        （CONFIRMED/REJECTED/WATCH/PENDING_TRIAGE）。
        """
        conn = self._conn()
        try:
            where, params = [], []
            if definition_id:
                where.append("r.definition_id=?")
                params.append(definition_id)
            if run_id:
                where.append("r.run_id=?")
                params.append(run_id)
            if not include_all_revisions:
                where.append(
                    "r.seq_no=(SELECT MAX(seq_no) FROM sig_signal_revisions x "
                    "WHERE x.signal_key=r.signal_key)")
            sql_w = (" WHERE " + " AND ".join(where)) if where else ""
            rows = conn.execute(
                "SELECT r.*, d.decision AS disp_decision, d.analyst AS disp_analyst,"
                " d.comment AS disp_comment, d.created_at AS disp_at "
                "FROM sig_signal_revisions r LEFT JOIN sig_signal_dispositions d "
                f"ON d.revision_id=r.id{sql_w} ORDER BY r.signal_key, r.seq_no DESC "
                "LIMIT ?", params + [limit]).fetchall()
            out = [self._format(r) for r in rows]
            if status:
                out = [d for d in out if d["triage_status"] == status]
            return out
        finally:
            conn.close()

    def get_signal(self, signal_key: str) -> dict:
        """一条信号的完整修订链（每修订带各自处置；旧结论原封保留）。"""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT r.*, d.decision AS disp_decision, d.analyst AS disp_analyst,"
                " d.comment AS disp_comment, d.created_at AS disp_at "
                "FROM sig_signal_revisions r LEFT JOIN sig_signal_dispositions d "
                "ON d.revision_id=r.id WHERE r.signal_key=? "
                "ORDER BY r.seq_no", (signal_key,)).fetchall()
            if not rows:
                raise SignalError("SIGNAL_NOT_FOUND",
                                  f"信号不存在: {signal_key}")
            revisions = [self._format(r) for r in rows]
            return {"signal_key": signal_key,
                    "definition_id": rows[-1]["definition_id"],
                    "drug_group_id": rows[-1]["drug_group_id"],
                    "event_group_id": rows[-1]["event_group_id"],
                    "latest_seq_no": rows[-1]["seq_no"],
                    "latest_triage_status": revisions[-1]["triage_status"],
                    "revisions": revisions}
        finally:
            conn.close()

    @staticmethod
    def _format(r) -> dict:
        d = {
            "revision_id": r["id"], "id": r["id"],
            "signal_key": r["signal_key"],
            "definition_id": r["definition_id"],
            "drug_group_id": r["drug_group_id"],
            "event_group_id": r["event_group_id"],
            "seq_no": r["seq_no"], "run_id": r["run_id"],
            "prev_revision_id": r["prev_revision_id"],
            "change_type": r["change_type"],
            "counts": json.loads(r["counts_json"]),
            "metrics": json.loads(r["metrics_json"]),
            "decision": json.loads(r["decision_json"]),
            "content_hash": r["content_hash"], "created_at": r["created_at"],
        }
        if r["disp_decision"]:
            d["disposition"] = {"decision": r["disp_decision"],
                                "analyst": r["disp_analyst"],
                                "comment": r["disp_comment"],
                                "created_at": r["disp_at"]}
            d["triage_status"] = r["disp_decision"]
        else:
            d["disposition"] = None
            d["triage_status"] = "PENDING_TRIAGE"
        return d
