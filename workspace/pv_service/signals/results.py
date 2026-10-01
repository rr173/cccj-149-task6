"""窗口结果查询、病例贡献追踪与修订间可追踪差异。

- ``get_run``：窗口快照（冻结配置、输入/输出哈希、四格指标清单）；
- ``run_cases``：入选（及落选）来源清单，可按药品组/事件组过滤到 a 格；
- ``source_contributions``：同一来源跨窗口的完整贡献轨迹（用了哪个版本、
  命中哪些组、归属哪个病例）——归并拆解、迟到随访、因果评价修正如何
  撤销旧贡献/计入新贡献由此可逐窗口对账；
- ``diff_runs``：两个窗口之间逐来源迁移（跨药品组/跨事件组/退出样本）
  与逐信号修订差异；
- ``revision_diff``：同一条信号两个修订号之间的字段级差异。
"""
from __future__ import annotations

import json

from .db import connect

_METRIC_KEYS = ("prr", "prr_ci95_lower", "prr_ci95_upper", "ror",
                "ror_ci95_lower", "ror_ci95_upper", "chi_square")


class ResultError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class ResultService:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _conn(self):
        return connect(self.db_path)

    # ------------------------------------------------------------------
    # 窗口结果
    # ------------------------------------------------------------------
    def list_runs(self, *, definition_id: str | None = None) -> list[dict]:
        conn = self._conn()
        try:
            where, params = "", []
            if definition_id:
                where, params = " WHERE definition_id=?", [definition_id]
            return [dict(r) for r in conn.execute(
                "SELECT id,job_id,computation_key,definition_id,definition_version_no,"
                "term_catalog_id,window_start,window_end,cutoff,mode,prior_run_id,"
                "total_sources,included_sources,input_hash,output_hash,published_at "
                f"FROM sig_runs{where} ORDER BY window_start, published_at", params)]
        finally:
            conn.close()

    def get_run(self, run_id: str) -> dict:
        conn = self._conn()
        try:
            r = conn.execute("SELECT * FROM sig_runs WHERE id=?",
                             (run_id,)).fetchone()
            if not r:
                raise ResultError("RUN_NOT_FOUND", f"窗口不存在: {run_id}")
            d = dict(r)
            d["manifest"] = json.loads(d.pop("manifest_json"))
            d["pairs"] = [self._format_pair(p) for p in conn.execute(
                "SELECT * FROM sig_run_pair_metrics WHERE run_id=? "
                "ORDER BY drug_group_id,event_group_id", (run_id,))]
            return d
        finally:
            conn.close()

    def get_pair(self, run_id: str, drug_group_id: str,
                 event_group_id: str) -> dict:
        conn = self._conn()
        try:
            r = conn.execute(
                "SELECT * FROM sig_run_pair_metrics WHERE run_id=? AND drug_group_id=? "
                "AND event_group_id=?", (run_id, drug_group_id, event_group_id)
            ).fetchone()
            if not r:
                raise ResultError("PAIR_NOT_FOUND", "该组对在窗口中不存在")
            return self._format_pair(r)
        finally:
            conn.close()

    @staticmethod
    def _format_pair(r) -> dict:
        return {
            "run_id": r["run_id"], "drug_group_id": r["drug_group_id"],
            "event_group_id": r["event_group_id"],
            "a": r["a"], "b": r["b"], "c": r["c"], "d": r["d"], "n": r["n"],
            "metrics": json.loads(r["metrics_json"]),
            "decision": json.loads(r["decision_json"]),
            "a_source_ids": json.loads(r["a_source_ids_json"]),
            "revision_id": r["revision_id"],
            "content_hash": r["content_hash"],
        }

    # ------------------------------------------------------------------
    # 入选病例清单
    # ------------------------------------------------------------------
    def run_cases(self, run_id: str, *, included_only: bool = True,
                  drug_group_id: str | None = None,
                  event_group_id: str | None = None) -> list[dict]:
        conn = self._conn()
        try:
            if not conn.execute("SELECT 1 FROM sig_runs WHERE id=?",
                                (run_id,)).fetchone():
                raise ResultError("RUN_NOT_FOUND", f"窗口不存在: {run_id}")
            rows = [self._format_case(r) for r in conn.execute(
                "SELECT * FROM sig_run_cases WHERE run_id=? ORDER BY source_id",
                (run_id,))]
            out = rows
            if included_only:
                out = [r for r in out if r["included"] == 1]
            if drug_group_id:
                out = [r for r in out if drug_group_id in r["drug_group_ids"]]
            if event_group_id:
                out = [r for r in out if event_group_id in r["event_group_ids"]]
            return out
        finally:
            conn.close()

    @staticmethod
    def _format_case(r) -> dict:
        return {
            "run_id": r["run_id"], "source_id": r["source_id"],
            "version_id": r["version_id"], "version_no": r["version_no"],
            "case_id": r["case_id"], "event_date": r["event_date"],
            "drug_group_ids": json.loads(r["drug_group_ids_json"]),
            "event_group_ids": json.loads(r["event_group_ids_json"]),
            "drug_codes": json.loads(r["drug_codes_json"]),
            "event_codes": json.loads(r["event_codes_json"]),
            "causality_levels": json.loads(r["causality_levels_json"]),
            "included": r["included"], "exclude_reason": r["exclude_reason"],
            "row_hash": r["row_hash"],
        }

    # ------------------------------------------------------------------
    # 病例贡献追踪
    # ------------------------------------------------------------------
    def source_contributions(self, source_id: str, *,
                             definition_id: str | None = None) -> dict:
        """一个来源在该定义全部窗口中的逐窗口贡献（含撤销/重计轨迹）。

        每个窗口条目记录冻结版本、归属病例、命中的组与指纹；相邻窗口的
        version_no / case_id / 组归属变化标注为对应修订类型
        （FOLLOWUP 迟到随访、CAUSALITY 因果修正、CASE_RELINK 归并拆解、
        GROUPS_CHANGED 跨组迁移、WITHDRAWN 退出样本、NEW 新进入）。
        """
        conn = self._conn()
        try:
            where, params = "", []
            if definition_id:
                where, params = "AND r.definition_id=?", [definition_id]
            rows = conn.execute(
                "SELECT r.id AS run_id,r.window_start,r.window_end,r.cutoff,"
                "r.definition_id,r.published_at,c.* FROM sig_run_cases c "
                "JOIN sig_runs r ON r.id=c.run_id WHERE c.source_id=? "
                + where + " ORDER BY r.window_start",
                [source_id] + params).fetchall()
            entries = []
            prev = None
            for r in rows:
                cur = self._format_case(r)
                cur.update({"window_start": r["window_start"],
                            "window_end": r["window_end"], "cutoff": r["cutoff"],
                            "published_at": r["published_at"]})
                cur["change_from_prior"] = self._row_change(prev, cur)
                entries.append(cur)
                prev = cur
            return {"source_id": source_id, "contributions": entries}
        finally:
            conn.close()

    @staticmethod
    def _row_change(prev: dict | None, cur: dict) -> str:
        if prev is None:
            return "NEW"
        if cur["included"] == 0 or cur["exclude_reason"] == "WITHDRAWN":
            return "WITHDRAWN"
        kind = []
        if prev["version_id"] != cur["version_id"]:
            kind.append("VERSION_BUMP")
        if prev["case_id"] != cur["case_id"]:
            kind.append("CASE_RELINK")
        if prev["drug_group_ids"] != cur["drug_group_ids"]:
            kind.append("DRUG_GROUP_CHANGED")
        if prev["event_group_ids"] != cur["event_group_ids"]:
            kind.append("EVENT_GROUP_CHANGED")
        if not kind:
            return "UNCHANGED"
        return "+".join(kind)

    # ------------------------------------------------------------------
    # 窗口差异与修订差异
    # ------------------------------------------------------------------
    def diff_runs(self, old_run_id: str, new_run_id: str) -> dict:
        """两个窗口间的可追踪差异：逐来源迁移 + 逐信号新旧对照。"""
        conn = self._conn()
        try:
            old_rows = {r["source_id"]: self._format_case(r) for r in conn.execute(
                "SELECT * FROM sig_run_cases WHERE run_id=?", (old_run_id,))}
            new_rows = {r["source_id"]: self._format_case(r) for r in conn.execute(
                "SELECT * FROM sig_run_cases WHERE run_id=?", (new_run_id,))}
            case_changes = {"added": [], "removed": [], "changed": []}
            for sid in sorted(new_rows.keys() - old_rows.keys()):
                case_changes["added"].append(self._brief(new_rows[sid]))
            for sid in sorted(old_rows.keys() - new_rows.keys()):
                case_changes["removed"].append(self._brief(old_rows[sid]))
            for sid in sorted(old_rows.keys() & new_rows.keys()):
                o, n = old_rows[sid], new_rows[sid]
                change = self._row_change(o, n)
                if change != "UNCHANGED":
                    entry = {"source_id": sid, "change": change,
                             "old": self._brief(o), "new": self._brief(n)}
                    if self._crosses_groups(o, n):
                        entry["crosses_group"] = True
                    case_changes["changed"].append(entry)

            old_pairs = {(p["drug_group_id"], p["event_group_id"]):
                         self._format_pair(p) for p in conn.execute(
                "SELECT * FROM sig_run_pair_metrics WHERE run_id=?", (old_run_id,))}
            new_pairs = {(p["drug_group_id"], p["event_group_id"]):
                         self._format_pair(p) for p in conn.execute(
                "SELECT * FROM sig_run_pair_metrics WHERE run_id=?", (new_run_id,))}
            signal_changes = {"new_signals": [], "withdrawn": [], "changed": [],
                              "unchanged": []}
            for k in sorted(new_pairs.keys() - old_pairs.keys()):
                signal_changes["new_signals"].append(self._pair_brief(new_pairs[k]))
            for k in sorted(old_pairs.keys() - new_pairs.keys()):
                signal_changes["withdrawn"].append(self._pair_brief(old_pairs[k]))
            for k in sorted(old_pairs.keys() & new_pairs.keys()):
                o, n = old_pairs[k], new_pairs[k]
                old_sig = o["decision"]["is_signal"]
                new_sig = n["decision"]["is_signal"]
                brief = {"drug_group_id": k[0], "event_group_id": k[1],
                         "old_revision_id": o["revision_id"],
                         "new_revision_id": n["revision_id"],
                         "old_counts": {kk: o[kk] for kk in ("a", "b", "c", "d")},
                         "new_counts": {kk: n[kk] for kk in ("a", "b", "c", "d")},
                         "old_decision": o["decision"]["decision"],
                         "new_decision": n["decision"]["decision"],
                         "old_metrics": self._metric_brief(o),
                         "new_metrics": self._metric_brief(n)}
                if old_sig and not new_sig:
                    brief["change"] = "WITHDRAWN"
                    signal_changes["withdrawn"].append(brief)
                elif o["content_hash"] != n["content_hash"]:
                    brief["change"] = ("SIGNAL_ON" if not old_sig and new_sig
                                       else "COUNT_CHANGE")
                    signal_changes["changed"].append(brief)
                else:
                    signal_changes["unchanged"].append(brief)
            return {"old_run_id": old_run_id, "new_run_id": new_run_id,
                    "case_changes": case_changes, "signal_changes": signal_changes}
        finally:
            conn.close()

    def revision_diff(self, old_revision_id: str,
                      new_revision_id: str) -> dict:
        """同一 signal_key 两个修订号之间的字段级差异。"""
        conn = self._conn()
        try:
            ro = conn.execute("SELECT * FROM sig_signal_revisions WHERE id=?",
                              (old_revision_id,)).fetchone()
            rn = conn.execute("SELECT * FROM sig_signal_revisions WHERE id=?",
                              (new_revision_id,)).fetchone()
            if not ro or not rn:
                raise ResultError("REVISION_NOT_FOUND", "信号修订不存在")
            if ro["signal_key"] != rn["signal_key"]:
                raise ResultError("DIFFERENT_SIGNAL", "两个修订不属于同一信号")
            co, cn = json.loads(ro["counts_json"]), json.loads(rn["counts_json"])
            mo, mn = json.loads(ro["metrics_json"]), json.loads(rn["metrics_json"])
            count_fields = {k: {"old": co[k], "new": cn[k]}
                            for k in ("a", "b", "c", "d", "n") if co[k] != cn[k]}
            metric_fields = {k: {"old": mo.get(k), "new": mn.get(k)}
                             for k in _METRIC_KEYS if mo.get(k) != mn.get(k)}
            return {
                "signal_key": ro["signal_key"],
                "old": {"revision_id": ro["id"], "seq_no": ro["seq_no"],
                        "run_id": ro["run_id"], "change_type": ro["change_type"],
                        "decision": json.loads(ro["decision_json"])["decision"]},
                "new": {"revision_id": rn["id"], "seq_no": rn["seq_no"],
                        "run_id": rn["run_id"], "change_type": rn["change_type"],
                        "decision": json.loads(rn["decision_json"])["decision"]},
                "count_fields": count_fields,
                "metric_fields": metric_fields,
                "decision_changed":
                    json.loads(ro["decision_json"])["decision"]
                    != json.loads(rn["decision_json"])["decision"],
            }
        finally:
            conn.close()

    @staticmethod
    def _brief(row: dict) -> dict:
        return {"source_id": row["source_id"], "case_id": row["case_id"],
                "version_no": row["version_no"],
                "drug_group_ids": row["drug_group_ids"],
                "event_group_ids": row["event_group_ids"],
                "included": row["included"], "exclude_reason": row["exclude_reason"]}

    @staticmethod
    def _crosses_groups(old: dict, new: dict) -> bool:
        return (set(old["drug_group_ids"]) != set(new["drug_group_ids"])
                or set(old["event_group_ids"]) != set(new["event_group_ids"]))

    @staticmethod
    def _pair_brief(p: dict) -> dict:
        return {"drug_group_id": p["drug_group_id"],
                "event_group_id": p["event_group_id"],
                "counts": {k: p[k] for k in ("a", "b", "c", "d")},
                "decision": p["decision"]["decision"],
                "revision_id": p["revision_id"],
                "metrics": ResultService._metric_brief(p)}

    @staticmethod
    def _metric_brief(p: dict) -> dict:
        return {k: p["metrics"].get(k) for k in
                ("prr", "prr_ci95_lower", "ror", "ror_ci95_lower", "chi_square")}
