"""监测定义：药品集合、事件分组、阈值；不可变版本化。

分析员每次修改都产生一个新版本（``sig_definition_versions``，只增）；
窗口作业冻结的是具体的 **definition_version_id**，所以旧窗口永远按旧
分组/旧阈值复现，分析员后续改动只影响后续窗口。
"""
from __future__ import annotations

import hashlib
import json
import uuid

from ..service import Clock
from . import audit
from .catalog import CatalogError, TermCatalog
from .db import transaction

# 允许配置的阈值键与取值约束
_THRESHOLD_KEYS = ("min_cases", "min_prr", "min_prr_ci_lower",
                   "min_ror", "min_ror_ci_lower", "min_chi_square")
_INT_KEYS = ("min_cases",)
_CAUSALITY_LEVELS = ("DEFINITE", "PROBABLE", "POSSIBLE", "UNLIKELY",
                     "UNRELATED", "UNASSESSED")


def _now(clock: Clock) -> str:
    return clock.now().isoformat(timespec="seconds")


def _uid() -> str:
    return uuid.uuid4().hex


def canonical_spec(spec: dict) -> str:
    return json.dumps(spec, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str)


def content_hash(spec: dict) -> str:
    return hashlib.sha256(canonical_spec(spec).encode("utf-8")).hexdigest()


class DefinitionError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def normalize_spec(spec: dict, catalog: TermCatalog | None = None) -> dict:
    """校验并规范化定义内容（组 id 排序、去重、阈值白名单）。

    若提供目录，则校验事件组的 ``concept_codes`` 与（可选）药品组的
    ``drug_codes``（DRUG 概念）都存在于冻结目录版本中。
    """
    if not isinstance(spec, dict):
        raise DefinitionError("BAD_SPEC", "spec 必须是对象")
    drug_groups = spec.get("drug_groups")
    event_groups = spec.get("event_groups")
    if not isinstance(drug_groups, list) or len(drug_groups) < 1:
        raise DefinitionError("BAD_SPEC", "drug_groups 至少包含一个药品组")
    if not isinstance(event_groups, list) or len(event_groups) < 1:
        raise DefinitionError("BAD_SPEC", "event_groups 至少包含一个事件组")

    def _norm_groups(groups, *, code_field, kind, what):
        out, group_ids = [], set()
        for g in groups:
            gid = g.get("id")
            name = g.get("name") or gid
            codes = g.get(code_field)
            if not gid or not isinstance(gid, str):
                raise DefinitionError("BAD_SPEC", f"{what}组缺少 id")
            if gid in group_ids:
                raise DefinitionError("DUP_GROUP", f"{what}组 id 重复: {gid}")
            group_ids.add(gid)
            if not isinstance(codes, list) or not codes:
                raise DefinitionError("BAD_SPEC",
                                      f"{what}组 {gid} 的 {code_field} 必须是非空数组")
            norm_codes = sorted({str(c) for c in codes})
            if catalog is not None:
                for c in norm_codes:
                    concept = catalog._by_code.get(c)
                    if concept is None:
                        raise CatalogError(
                            "UNKNOWN_CODE",
                            f"{what}组 {gid} 引用了目录中不存在的代码: {c}")
                    if concept["kind"] != kind:
                        raise CatalogError(
                            "KIND_MISMATCH",
                            f"{what}组 {gid} 的代码 {c} 不是 {kind} 概念")
            out.append({"id": gid, "name": str(name), code_field: norm_codes})
        return sorted(out, key=lambda g: g["id"])

    norm = {
        "drug_groups": _norm_groups(
            drug_groups, code_field="drug_codes", kind="DRUG", what="药品"),
        "event_groups": _norm_groups(
            event_groups, code_field="concept_codes", kind="EVENT", what="事件"),
    }

    thresholds_in = spec.get("thresholds") or {}
    if not isinstance(thresholds_in, dict):
        raise DefinitionError("BAD_SPEC", "thresholds 必须是对象")
    thresholds = {}
    for k, v in thresholds_in.items():
        if k not in _THRESHOLD_KEYS:
            raise DefinitionError("BAD_THRESHOLD", f"未知阈值: {k}")
        if v is None:
            continue
        if k in _INT_KEYS:
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise DefinitionError("BAD_THRESHOLD", f"{k} 必须是非负整数")
            thresholds[k] = v
        else:
            if not isinstance(v, (int, float)) or isinstance(v, bool) or v < 0:
                raise DefinitionError("BAD_THRESHOLD", f"{k} 必须是非负数")
            thresholds[k] = float(v)
    norm["thresholds"] = {k: thresholds.get(k) for k in _THRESHOLD_KEYS
                          if k in thresholds}

    excluded = spec.get("excluded_causality_levels") or []
    if not isinstance(excluded, list):
        raise DefinitionError("BAD_SPEC",
                              "excluded_causality_levels 必须是数组")
    for lvl in excluded:
        if lvl not in _CAUSALITY_LEVELS:
            raise DefinitionError("BAD_CAUSALITY",
                                  f"未知因果评价等级: {lvl}（合法值 {_CAUSALITY_LEVELS}）")
    norm["excluded_causality_levels"] = sorted(set(excluded))
    return norm


class DefinitionService:
    def __init__(self, db_path: str, clock: Clock):
        self.db_path = db_path
        self.clock = clock

    def _conn(self):
        from .db import connect
        return connect(self.db_path)

    def create(self, definition_id: str, spec: dict, *, name: str | None = None,
               actor: str | None = None, term_catalog_id: str | None = None) -> dict:
        """创建监测定义并写入首个版本（可提供目录做代码校验）。"""
        catalog = self._catalog(term_catalog_id)
        norm = normalize_spec(spec, catalog)
        now = _now(self.clock)
        with transaction(self._conn()) as conn:
            if conn.execute("SELECT 1 FROM sig_definitions WHERE id=?",
                            (definition_id,)).fetchone():
                raise DefinitionError("DEFINITION_EXISTS", f"定义已存在: {definition_id}")
            h = content_hash(norm)
            version_id = _uid()
            conn.execute(
                "INSERT INTO sig_definitions(id,name,current_version_no,created_by,"
                "created_at,updated_at) VALUES(?,?,?,?,?,?)",
                (definition_id, name or definition_id, 1, actor, now, now))
            conn.execute(
                "INSERT INTO sig_definition_versions(id,definition_id,version_no,"
                "spec_json,content_hash,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (version_id, definition_id, 1, canonical_spec(norm), h, actor, now))
            audit.append_event(
                conn, "DEFINITION_CREATED", f"创建监测定义 {definition_id} v1",
                payload={"definition_id": definition_id, "version_id": version_id,
                         "content_hash": h},
                actor=actor, when=now,
                idempotency_key=f"defver:{definition_id}:1:{h}")
            return {"id": definition_id, "version_id": version_id,
                    "version_no": 1, "deduped": False}

    def new_version(self, definition_id: str, spec: dict, *,
                    actor: str | None = None,
                    term_catalog_id: str | None = None) -> dict:
        """为既有定义追加新版本；内容与当前版本完全相同则返回当前版本。"""
        catalog = self._catalog(term_catalog_id)
        norm = normalize_spec(spec, catalog)
        h = content_hash(norm)
        now = _now(self.clock)
        with transaction(self._conn()) as conn:
            row = conn.execute(
                "SELECT current_version_no FROM sig_definitions WHERE id=?",
                (definition_id,)).fetchone()
            if not row:
                raise DefinitionError("DEFINITION_NOT_FOUND",
                                      f"定义不存在: {definition_id}")
            cur = conn.execute(
                "SELECT id,version_no,content_hash FROM sig_definition_versions "
                "WHERE definition_id=? ORDER BY version_no DESC LIMIT 1",
                (definition_id,)).fetchone()
            if cur["content_hash"] == h:
                return {"id": definition_id, "version_id": cur["id"],
                        "version_no": cur["version_no"], "deduped": True}
            next_no = cur["version_no"] + 1
            version_id = _uid()
            conn.execute(
                "INSERT INTO sig_definition_versions(id,definition_id,version_no,"
                "spec_json,content_hash,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (version_id, definition_id, next_no, canonical_spec(norm), h,
                 actor, now))
            conn.execute(
                "UPDATE sig_definitions SET current_version_no=?, updated_at=? "
                "WHERE id=?", (next_no, now, definition_id))
            audit.append_event(
                conn, "DEFINITION_VERSIONED",
                f"监测定义 {definition_id} 追加版本 v{next_no}",
                payload={"definition_id": definition_id, "version_id": version_id,
                         "version_no": next_no, "content_hash": h},
                actor=actor, when=now,
                idempotency_key=f"defver:{definition_id}:{next_no}:{h}")
            return {"id": definition_id, "version_id": version_id,
                    "version_no": next_no, "deduped": False}

    def _catalog(self, term_catalog_id: str | None) -> TermCatalog | None:
        if not term_catalog_id:
            return None
        return TermCatalog.load(self._conn(), term_catalog_id)

    def list_definitions(self) -> list[dict]:
        conn = self._conn()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT id,name,current_version_no,created_by,created_at,updated_at "
                "FROM sig_definitions ORDER BY created_at")]
        finally:
            conn.close()

    def get(self, definition_id: str, *, version_no: int | None = None) -> dict:
        conn = self._conn()
        try:
            d = conn.execute("SELECT * FROM sig_definitions WHERE id=?",
                             (definition_id,)).fetchone()
            if not d:
                raise DefinitionError("DEFINITION_NOT_FOUND",
                                      f"定义不存在: {definition_id}")
            if version_no is None:
                version_no = d["current_version_no"]
            v = conn.execute(
                "SELECT * FROM sig_definition_versions WHERE definition_id=? "
                "AND version_no=?", (definition_id, version_no)).fetchone()
            if not v:
                raise DefinitionError("VERSION_NOT_FOUND",
                                      f"定义 {definition_id} 无版本 v{version_no}")
            out = dict(d)
            out["version"] = {
                "id": v["id"], "version_no": v["version_no"],
                "spec": json.loads(v["spec_json"]),
                "content_hash": v["content_hash"], "created_by": v["created_by"],
                "created_at": v["created_at"],
            }
            return out
        finally:
            conn.close()

    @staticmethod
    def load_spec(conn, definition_version_id: str) -> dict:
        """读取指定版本 id 的规范内容（计算路径使用）。"""
        r = conn.execute(
            "SELECT definition_id, version_no, spec_json, content_hash "
            "FROM sig_definition_versions WHERE id=?",
            (definition_version_id,)).fetchone()
        if not r:
            raise DefinitionError("VERSION_NOT_FOUND",
                                  f"定义版本不存在: {definition_version_id}")
        spec = json.loads(r["spec_json"])
        spec["_meta"] = {"definition_id": r["definition_id"],
                         "version_no": r["version_no"],
                         "content_hash": r["content_hash"]}
        return spec
