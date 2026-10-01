"""术语归一、监测定义版本与来源行构建的测试。"""
import json
import unittest

from pv_service.signals.build import build_source_row, normalize_causality
from pv_service.signals.catalog import (CatalogError, CatalogService,
                                        TermCatalog, fold_term)
from pv_service.signals.definitions import (DefinitionError, DefinitionService,
                                            normalize_spec)
from tests.signals_base import CATALOG, DEFAULT_SPEC

# 不初始化数据库也能做大部分纯函数测试


class TestFoldTerm(unittest.TestCase):
    def test_nfkc_casefold_strip(self):
        self.assertEqual(fold_term(" Rash "), "rash")
        self.assertEqual(fold_term("ＲＡＳＨ"), "rash")  # 全角
        self.assertEqual(fold_term("ÌBU"), "ìbu")
        self.assertEqual(fold_term(None), "")


class TestCatalogValidation(unittest.TestCase):
    def test_duplicate_code_rejected(self):
        spec = {"concepts": [
            {"code": "X", "kind": "EVENT", "terms": ["a"]},
            {"code": "X", "kind": "EVENT", "terms": ["b"]}]}
        with self.assertRaises(CatalogError) as cm:
            from pv_service.signals.catalog import validate_spec
            validate_spec(spec)
        self.assertEqual(cm.exception.code, "DUP_CODE")

    def test_duplicate_folded_term_rejected(self):
        spec = {"concepts": [
            {"code": "X", "kind": "EVENT", "terms": [" Rash "]},
            {"code": "Y", "kind": "EVENT", "terms": ["RASH"]}]}
        from pv_service.signals.catalog import validate_spec
        with self.assertRaises(CatalogError) as cm:
            validate_spec(spec)
        self.assertEqual(cm.exception.code, "DUP_TERM")

    def test_bad_kind(self):
        from pv_service.signals.catalog import validate_spec
        with self.assertRaises(CatalogError):
            validate_spec({"concepts": [{"code": "X", "kind": "OTHER",
                                         "terms": ["a"]}]})


class TestTermResolution(unittest.TestCase):
    def setUp(self):
        self.cat = TermCatalog("t", CATALOG)

    def test_term_and_alias(self):
        self.assertEqual(self.cat.resolve("布洛芬", kind="EVENT" if False else "DRUG")["code"],
                         "IBU")
        self.assertEqual(self.cat.resolve("ibuprofeno")["code"], "IBU")
        self.assertEqual(self.cat.resolve("ＲASH".replace("Ｒ", "R"))["code"],
                         "RASH")

    def test_explicit_code(self):
        r = self.cat.resolve(None, code="RASH", kind="EVENT")
        self.assertEqual(r["via"], "CODE")

    def test_unknown_code(self):
        with self.assertRaises(CatalogError) as cm:
            self.cat.resolve(None, code="NOPE")
        self.assertEqual(cm.exception.code, "UNKNOWN_CODE")

    def test_kind_mismatch(self):
        with self.assertRaises(CatalogError):
            self.cat.resolve(None, code="RASH", kind="DRUG")
        self.assertFalse(self.cat.resolve("皮疹", kind="DRUG")["matched"])

    def test_unmapped(self):
        r = self.cat.resolve("头疼")
        self.assertFalse(r["matched"])
        self.assertIsNone(r["code"])


class TestDefinitionNormalize(unittest.TestCase):
    def test_groups_sorted_and_dedup(self):
        spec = {
            "drug_groups": [{"id": "B", "drug_codes": ["z", "a", "a"]},
                            {"id": "A", "drug_codes": ["q"]}],
            "event_groups": [{"id": "E", "concept_codes": ["RASH"]}]}
        norm = normalize_spec(spec)
        self.assertEqual(norm["drug_groups"][0]["id"], "A")
        self.assertEqual(norm["drug_groups"][1]["drug_codes"], ["a", "z"])

    def test_threshold_whitelist(self):
        with self.assertRaises(DefinitionError):
            normalize_spec({"drug_groups": [{"id": "D", "drug_codes": ["x"]}],
                            "event_groups": [{"id": "E", "concept_codes": ["y"]}],
                            "thresholds": {"bogus": 1}})

    def test_bad_causality_level(self):
        with self.assertRaises(DefinitionError):
            normalize_spec({"drug_groups": [{"id": "D", "drug_codes": ["x"]}],
                            "event_groups": [{"id": "E", "concept_codes": ["y"]}],
                            "excluded_causality_levels": ["MAYBE"]})

    def test_codes_checked_against_catalog(self):
        cat = TermCatalog("t", CATALOG)
        bad = {"drug_groups": [{"id": "D", "drug_codes": ["GHOST"]}],
               "event_groups": [{"id": "E", "concept_codes": ["RASH"]}]}
        with self.assertRaises(CatalogError):
            normalize_spec(bad, cat)
        bad2 = {"drug_groups": [{"id": "D", "drug_codes": ["RASH"]}],
                "event_groups": [{"id": "E", "concept_codes": ["RASH"]}]}
        with self.assertRaises(CatalogError):
            normalize_spec(bad2, cat)  # EVENT 代码不能放药品组


class TestCausality(unittest.TestCase):
    def test_aliases(self):
        self.assertEqual(normalize_causality("可能"), "POSSIBLE")
        self.assertEqual(normalize_causality("probable"), "PROBABLE")
        self.assertEqual(normalize_causality("无关"), "UNRELATED")
        self.assertEqual(normalize_causality(None), "UNASSESSED")
        self.assertEqual(normalize_causality("??"), "UNASSESSED")


class TestBuildSourceRow(unittest.TestCase):
    def setUp(self):
        self.cat = TermCatalog("t", CATALOG)
        self.spec = normalize_spec(DEFAULT_SPEC, self.cat)

    def build(self, payload, sid="s1"):
        return build_source_row(
            source_id=sid, version_id="v1", version_no=1, case_id="c1",
            event_date="2026-09-01", payload=payload,
            catalog=self.cat, spec=self.spec)

    def test_membership(self):
        row = self.build({"drugs": [{"name": "布洛芬"}],
                          "events": [{"term": "皮疹"}]})
        self.assertEqual(row["drug_group_ids"], ["D_IBU"])
        self.assertEqual(row["event_group_ids"], ["E_RASH"])
        self.assertEqual(row["drug_codes"], ["IBU"])
        self.assertEqual(row["event_codes"], ["RASH"])

    def test_multi_group_membership(self):
        row = self.build({"drugs": [{"name": "布洛芬"}, {"name": "阿莫西林"}],
                          "events": [{"term": "皮疹"}, {"term": "恶心"}]})
        self.assertEqual(row["drug_group_ids"], ["D_AMX", "D_IBU"])
        self.assertEqual(row["event_group_ids"], ["E_GI", "E_RASH"])

    def test_unmapped_drug_is_background(self):
        row = self.build({"drugs": [{"name": "未知药"}],
                          "events": [{"term": "皮疹"}]})
        self.assertEqual(row["drug_group_ids"], [])
        self.assertEqual(row["event_group_ids"], ["E_RASH"])

    def test_causality_exclusion_removes_exposure(self):
        spec = dict(self.spec)
        spec["excluded_causality_levels"] = ["UNRELATED"]
        row = build_source_row(
            source_id="s", version_id="v", version_no=1, case_id="c",
            event_date="2026-09-01",
            payload={"drugs": [{"name": "布洛芬", "causality": "无关"}],
                     "events": [{"term": "皮疹"}]},
            catalog=self.cat, spec=spec)
        self.assertEqual(row["drug_group_ids"], [])
        self.assertEqual(row["event_group_ids"], ["E_RASH"])
        self.assertIn("UNRELATED", row["causality_levels"])

    def test_most_severe_causality_wins(self):
        spec = dict(self.spec)
        spec["excluded_causality_levels"] = ["UNRELATED"]
        # 同一标准药两次：一次无关、一次可能 -> 仍算暴露（取最相关评价）
        row = build_source_row(
            source_id="s", version_id="v", version_no=1, case_id="c",
            event_date="2026-09-01",
            payload={"drugs": [{"name": "布洛芬", "causality": "unrelated"},
                               {"name": "布洛芬", "causality": "possible"}],
                     "events": [{"term": "皮疹"}]},
            catalog=self.cat, spec=spec)
        self.assertEqual(row["drug_group_ids"], ["D_IBU"])

    def test_row_hash_stable(self):
        payload = {"drugs": [{"name": "布洛芬"}], "events": [{"term": "皮疹"}]}
        r1 = self.build(payload)
        r2 = self.build(payload)
        self.assertEqual(r1["row_hash"], r2["row_hash"])
        # case_id 不影响指纹（归并拆解不触发重算）
        r3 = build_source_row(
            source_id="s1", version_id="v1", version_no=1, case_id="OTHER",
            event_date="2026-09-01", payload=payload,
            catalog=self.cat, spec=self.spec)
        self.assertEqual(r1["row_hash"], r3["row_hash"])
        # 跨事件组的修订改变指纹
        r4 = self.build({"drugs": [{"name": "布洛芬"}],
                         "events": [{"term": "恶心"}]})
        self.assertNotEqual(r1["row_hash"], r4["row_hash"])


if __name__ == "__main__":
    unittest.main()
