"""术语归一版本：登记不可变目录并把药品名/事件术语解析为标准代码。

一次窗口运行在提交作业时**冻结** ``term_catalog_id``：所有归一解析都只
针对该版本的概念快照，之后再登记新目录版本不影响任何历史运行的可复现性。

归一匹配：对目录术语和输入术语做 NFKC Unicode 折叠 + ``casefold`` +
去首尾空白后精确匹配（与 ``identity.py`` 的身份折叠同理：全角、大小写、
组合写法差异稳定指向同一概念）。显式给出的标准代码优先，不做名称回退。
相同内容（``content_hash``）重复登记返回原版本（``deduped=True``）。
"""
from __future__ import annotations

import hashlib
import json
import unicodedata
import uuid
from typing import Any

from ..service import Clock
from . import audit
from .db import transaction


def _now(clock: Clock) -> str:
    return clock.now().isoformat(timespec="seconds")


def _uid() -> str:
    return uuid.uuid4().hex


def canonical_spec(spec: dict) -> str:
    """目录内容的规范 JSON（排序键、紧凑分隔，UTF-8）。"""
    return json.dumps(spec, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str)


def content_hash(spec: dict) -> str:
    return hashlib.sha256(canonical_spec(spec).encode("utf-8")).hexdigest()


def fold_term(value: Any) -> str:
    """术语折叠：NFKC + casefold + 首尾空白。"""
    if value is None:
        return ""
    return unicodedata.normalize("NFKC", str(value)).casefold().strip()


def validate_spec(spec: dict) -> None:
    if not isinstance(spec, dict) or not isinstance(spec.get("concepts"), list):
        raise CatalogError("BAD_SPEC", "spec 必须包含 concepts 数组")
    seen_codes, seen_terms = set(), set()
    for i, c in enumerate(spec["concepts"]):
        if not isinstance(c, dict):
            raise CatalogError("BAD_SPEC", f"concepts[{i}] 必须是对象")
        code = c.get("code")
        kind = c.get("kind")
        if not code or not isinstance(code, str):
            raise CatalogError("BAD_SPEC", f"concepts[{i}].code 必填且为字符串")
        if kind not in ("DRUG", "EVENT"):
            raise CatalogError("BAD_SPEC",
                               f"concepts[{i}].kind 必须是 DRUG 或 EVENT")
        if code in seen_codes:
            raise CatalogError("DUP_CODE", f"概念代码重复: {code}")
        seen_codes.add(code)
        terms = c.get("terms") or []
        if not isinstance(terms, list):
            raise CatalogError("BAD_SPEC", f"概念 {code} 的 terms 必须是数组")
        for t in terms:
            folded = fold_term(t)
            if not folded:
                raise CatalogError("BAD_SPEC", f"概念 {code} 存在空术语")
            if folded in seen_terms:
                raise CatalogError("DUP_TERM",
                                   f"归一后术语重复（跨概念会歧义）: {t!r}")
            seen_terms.add(folded)


class CatalogError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class TermCatalog:
    """某个已登记目录版本的解析视图（不可变）。"""

    def __init__(self, catalog_id: str, spec: dict):
        self.id = catalog_id
        self.spec = spec
        self._by_code: dict[str, dict] = {}
        self._term_index: dict[str, str] = {}
        for c in spec.get("concepts", []):
            self._by_code[c["code"]] = c
            for t in c.get("terms", []):
                self._term_index[fold_term(t)] = c["code"]

    @classmethod
    def load(cls, conn, catalog_id: str) -> "TermCatalog":
        row = conn.execute(
            "SELECT id, spec_json FROM sig_term_catalogs WHERE id=?",
            (catalog_id,)).fetchone()
        if not row:
            raise CatalogError("CATALOG_NOT_FOUND", f"术语目录不存在: {catalog_id}")
        return cls(row["id"], json.loads(row["spec_json"]))

    def resolve(self, value: Any, *, code: Any = None, kind: str | None = None,
                label: str = "") -> dict[str, Any]:
        """把一个药品/事件归一到概念。

        显式 ``code`` 优先（必须存在、kind 匹配）；否则按折叠术语查索引。
        返回 ``{"code": str|None, "matched": bool, "via": "CODE"|"TERM"|null}``。
        """
        if code:
            concept = self._by_code.get(str(code))
            if concept is None:
                raise CatalogError("UNKNOWN_CODE",
                                   f"{label}引用了目录中不存在的代码: {code}")
            if kind and concept["kind"] != kind:
                raise CatalogError("KIND_MISMATCH",
                                   f"{code} 是 {concept['kind']} 概念，期望 {kind}")
            return {"code": str(code), "matched": True, "via": "CODE"}
        folded = fold_term(value)
        hit = self._term_index.get(folded) if folded else None
        if hit is None:
            return {"code": None, "matched": False, "via": None}
        if kind and self._by_code[hit]["kind"] != kind:
            return {"code": None, "matched": False, "via": None}
        return {"code": hit, "matched": True, "via": "TERM"}


class CatalogService:
    def __init__(self, db_path: str, clock: Clock):
        self.db_path = db_path
        self.clock = clock

    def register(self, catalog_id: str, spec: dict, *, name: str | None = None,
                 actor: str | None = None) -> dict:
        """登记不可变术语目录版本（相同内容返回原版本）。"""
        validate_spec(spec)
        h = content_hash(spec)
        with transaction(self._conn()) as conn:
            dup = conn.execute(
                "SELECT id FROM sig_term_catalogs WHERE content_hash=?",
                (h,)).fetchone()
            if dup:
                return {"id": dup["id"], "deduped": True}
            clash = conn.execute(
                "SELECT id FROM sig_term_catalogs WHERE id=?",
                (catalog_id,)).fetchone()
            if clash:
                raise CatalogError("ID_CONFLICT",
                                   f"目录 id 已被不同内容占用: {catalog_id}")
            conn.execute(
                "INSERT INTO sig_term_catalogs(id,name,spec_json,content_hash,"
                "created_by,created_at) VALUES(?,?,?,?,?,?)",
                (catalog_id, name or catalog_id, canonical_spec(spec), h,
                 actor, _now(self.clock)))
            audit.append_event(
                conn, "TERM_CATALOG_REGISTERED",
                f"登记术语归一版本 {catalog_id}",
                payload={"catalog_id": catalog_id, "content_hash": h,
                         "concepts": len(spec.get("concepts", []))},
                actor=actor, when=_now(self.clock),
                idempotency_key=f"catalog:{h}")
            return {"id": catalog_id, "deduped": False}

    def list_catalogs(self) -> list[dict]:
        conn = self._conn()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT id,name,content_hash,created_by,created_at "
                "FROM sig_term_catalogs ORDER BY created_at")]
        finally:
            conn.close()

    def get(self, catalog_id: str) -> dict:
        conn = self._conn()
        try:
            r = conn.execute(
                "SELECT id,name,spec_json,content_hash,created_by,created_at "
                "FROM sig_term_catalogs WHERE id=?", (catalog_id,)).fetchone()
            if not r:
                raise CatalogError("CATALOG_NOT_FOUND", f"术语目录不存在: {catalog_id}")
            d = dict(r)
            d["spec"] = json.loads(d.pop("spec_json"))
            return d
        finally:
            conn.close()

    def open(self, catalog_id: str) -> TermCatalog:
        conn = self._conn()
        try:
            return TermCatalog.load(conn, catalog_id)
        finally:
            conn.close()

    def _conn(self):
        from .db import connect
        return connect(self.db_path)
