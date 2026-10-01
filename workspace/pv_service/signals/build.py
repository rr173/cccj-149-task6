"""窗口四格的纯函数构建器（冻结输入 -> 逐来源行 -> 组对计数）。

所有函数不碰数据库，便于单测与“重新计算核验”：
``build_source_row`` 把一条**冻结的来源版本快照**按冻结的术语目录与定义
版本解析成一行（命中的药品组/事件组、标准代码、因果等级、是否入选）；
``aggregate_pairs`` 把全部来源行汇总为每个 (药品组,事件组) 的四格与指标。

因果评价等级：``DEFINITE/PROBABLE/POSSIBLE/UNLIKELY/UNRELATED/UNASSESSED``。
定义里的 ``excluded_causality_levels``（如 UNRELATED）只影响 *该药品是否
视为暴露*（因果评价修正后从 a/b 移到 c/d），不把来源整体踢出窗口样本——
同一来源在一个窗口仍只贡献一次。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from .catalog import TermCatalog, fold_term
from .metrics import compute_metrics, decide

CAUSALITY_ALIASES = {
    "DEFINITE": "DEFINITE", "肯定": "DEFINITE", "确定": "DEFINITE",
    "PROBABLE": "PROBABLE", "很可能": "PROBABLE",
    "POSSIBLE": "POSSIBLE", "可能": "POSSIBLE",
    "UNLIKELY": "UNLIKELY", "不太可能": "UNLIKELY", "可疑": "UNLIKELY",
    "UNRELATED": "UNRELATED", "无关": "UNRELATED", "不可能": "UNRELATED",
    "UNASSESSED": "UNASSESSED", "未评价": "UNASSESSED", "": "UNASSESSED",
}
CAUSALITY_LEVELS = tuple(
    lvl for lvl in CAUSALITY_ALIASES if lvl and lvl == lvl.upper())


def normalize_causality(value: Any) -> str:
    """因果评价归一（支持中文别名）；无法识别按 UNASSESSED（保守不排除）。"""
    folded = fold_term(value).upper()
    return CAUSALITY_ALIASES.get(folded, "UNASSESSED")


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str)


def row_hash(row: dict) -> str:
    """来源行指纹：决定该来源对窗口统计的全部解析结果。

    只覆盖与统计相关的字段（组归属/代码/因果/入选），不含 case_id；
    归并拆解（case 变化但组归属不变）通过归属快照单独追踪，不会因此
    重算四格，而跨药品组/事件组的迁移会改指纹 -> 增量分片变脏。
    """
    material = {
        "version_id": row["version_id"],
        "drug_group_ids": row["drug_group_ids"],
        "event_group_ids": row["event_group_ids"],
        "drug_codes": row["drug_codes"],
        "event_codes": row["event_codes"],
        "causality_levels": row["causality_levels"],
        "included": row["included"],
        "exclude_reason": row.get("exclude_reason"),
    }
    return hashlib.sha256(canonical_json(material).encode("utf-8")).hexdigest()


def build_source_row(*, source_id: str, version_id: str, version_no: int,
                     case_id: str, event_date: str, payload: dict,
                     catalog: TermCatalog, spec: dict) -> dict:
    """把一条冻结来源版本解析为窗口行。

    payload 结构（宽松）：
      {"drugs": [{"code"?, "name"?, "causality"?, "causality_level"?}],
       "events": [{"code"?, "term"|"name"?, "causality"?, "causality_level"?}]}
    顶层 "causality"/"causality_level" 作为未逐项标注时的缺省因果等级。
    """
    excluded_levels = set(spec.get("excluded_causality_levels") or [])
    default_causality = normalize_causality(
        payload.get("causality_level", payload.get("causality")))

    drug_codes: list[str] = []
    drug_levels: list[str] = []
    for i, d in enumerate(payload.get("drugs") or []):
        if not isinstance(d, dict):
            raise RowBuildError("BAD_DRUG", f"drugs[{i}] 必须是对象")
        res = catalog.resolve(d.get("name"), code=d.get("code"), kind="DRUG",
                              label=f"drugs[{i}]")
        if not res["matched"]:
            # 未归一药品不进入任何受监测药品组，但保留痕迹在 excluded 之外：
            # 它仍可作为“其他药品”背景（c/d），故不记录代码。
            continue
        level = normalize_causality(
            d.get("causality_level", d.get("causality", default_causality)))
        drug_codes.append(res["code"])
        drug_levels.append(level)

    event_codes: list[str] = []
    for i, e in enumerate(payload.get("events") or []):
        if not isinstance(e, dict):
            raise RowBuildError("BAD_EVENT", f"events[{i}] 必须是对象")
        res = catalog.resolve(e.get("term", e.get("name")),
                              code=e.get("code"), kind="EVENT",
                              label=f"events[{i}]")
        if res["matched"]:
            event_codes.append(res["code"])

    drug_codes_set = sorted(set(drug_codes))
    event_codes_set = sorted(set(event_codes))

    # 药品 -> 因果等级（同一标准药多次出现取最严重，即“更相关”优先）：
    # 只要有一次用药未被排除，该药品就算暴露。
    level_rank = {lvl: i for i, lvl in enumerate(
        ("UNRELATED", "UNLIKELY", "UNASSESSED", "POSSIBLE", "PROBABLE",
         "DEFINITE"))}
    code_level: dict[str, str] = {}
    for code, level in zip(drug_codes, drug_levels):
        if code not in code_level or level_rank[level] > level_rank[code_level[code]]:
            code_level[code] = level
    exposed_codes = sorted(
        c for c in drug_codes_set if code_level[c] not in excluded_levels)
    causality_levels = sorted({code_level[c] for c in drug_codes_set})

    drug_group_ids, event_group_ids = _membership(
        spec, exposed_codes, event_codes_set)

    row = {
        "source_id": source_id,
        "version_id": version_id,
        "version_no": version_no,
        "case_id": case_id,
        "event_date": event_date,
        "drug_group_ids": drug_group_ids,
        "event_group_ids": event_group_ids,
        "drug_codes": drug_codes_set,
        "event_codes": event_codes_set,
        "causality_levels": causality_levels,
        "included": 1,
        "exclude_reason": None,
    }
    row["row_hash"] = row_hash(row)
    return row


def _membership(spec: dict, exposed_codes: list[str],
                event_codes: list[str]) -> tuple[list[str], list[str]]:
    drug_set = set(exposed_codes)
    event_set = set(event_codes)
    dg = sorted(g["id"] for g in spec["drug_groups"]
                if drug_set.intersection(g["drug_codes"]))
    eg = sorted(g["id"] for g in spec["event_groups"]
                if event_set.intersection(g["concept_codes"]))
    return dg, eg


class RowBuildError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# ---------------------------------------------------------------------------
# 四格聚合
# ---------------------------------------------------------------------------
def aggregate_pairs(rows: list[dict], spec: dict) -> list[dict]:
    """对定义中每个 (药品组,事件组) 构建四格并计算指标/判定。

    计数单位是 **来源**（同一来源一行，组集合去重），因此合并病例不会
    重复计数。a 格同时返回命中的来源 id 清单（入选病例清单）。
    """
    included = [r for r in rows if r.get("included") == 1]
    n = len(included)
    thresholds = spec.get("thresholds") or {}
    out = []
    for dg in sorted(spec["drug_groups"], key=lambda g: g["id"]):
        for eg in sorted(spec["event_groups"], key=lambda g: g["id"]):
            a_ids, b_ids, c_ids, d_ids = [], [], [], []
            for r in included:
                in_drug = dg["id"] in r["drug_group_ids"]
                in_event = eg["id"] in r["event_group_ids"]
                if in_drug and in_event:
                    a_ids.append(r["source_id"])
                elif in_drug and not in_event:
                    b_ids.append(r["source_id"])
                elif not in_drug and in_event:
                    c_ids.append(r["source_id"])
                else:
                    d_ids.append(r["source_id"])
            a, b, c, d = len(a_ids), len(b_ids), len(c_ids), len(d_ids)
            counts = compute_metrics(a, b, c, d)
            decision = decide(counts, thresholds)
            out.append({
                "drug_group_id": dg["id"],
                "event_group_id": eg["id"],
                "a": a, "b": b, "c": c, "d": d, "n": n,
                "counts": counts,
                "decision": decision,
                "a_source_ids": sorted(a_ids),
            })
    return out
