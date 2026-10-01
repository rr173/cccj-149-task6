"""迟到随访、归并拆解、因果修正、退出样本的跨窗口追踪与差异。"""
import concurrent.futures
import unittest

from pv_service.signals.db import connect as sig_connect
from tests.signals_base import BaseSignalTest


def make_signal_dataset(t):
    for i in range(1, 9):
        t.source(f"r{i}", ["布洛芬"], ["皮疹"], f"2026-09-{i:02d}")
    t.source("g1", ["布洛芬"], ["恶心"], "2026-09-09")
    t.source("g2", ["布洛芬"], ["恶心"], "2026-09-10")
    t.source("g3", ["阿莫西林"], ["皮疹"], "2026-09-11")
    for i, sid in enumerate(["g4", "g5", "g6", "g7", "g8"]):
        t.source(sid, ["阿莫西林"], ["恶心"], f"2026-09-{12+i:02d}")


class TestRevisionsAndContributions(BaseSignalTest):
    def setUp(self):
        super().setUp()
        make_signal_dataset(self)
        self.run1 = self.run_of(self.submit_full(self.W1))

    def test_late_followup_cross_event_group_diff(self):
        self.clock.advance(days=20)
        self.followup("r1", ["布洛芬"], ["恶心"])
        run2 = self.run_of(self.submit_full(self.W2))
        diff = self.app.results.diff_runs(self.run1["id"], run2["id"])
        changed = {c["source_id"]: c for c in diff["case_changes"]["changed"]}
        self.assertIn("r1", changed)
        self.assertIn("EVENT_GROUP_CHANGED", changed["r1"]["change"])
        self.assertTrue(changed["r1"]["crosses_group"])
        # 旧组 a 格撤销 r1，新组 a 格计入 r1
        sig = {(c["drug_group_id"], c["event_group_id"]): c
               for c in diff["signal_changes"]["changed"]}
        self.assertEqual(sig[("D_IBU", "E_RASH")]["old_counts"]["a"]
                         - sig[("D_IBU", "E_RASH")]["new_counts"]["a"], 1)
        self.assertEqual(sig[("D_IBU", "E_GI")]["new_counts"]["a"]
                         - sig[("D_IBU", "E_GI")]["old_counts"]["a"], 1)
        # 贡献轨迹
        contrib = self.app.results.source_contributions("r1")
        changes = [c["change_from_prior"] for c in contrib["contributions"]]
        self.assertEqual(changes, ["NEW", "VERSION_BUMP+EVENT_GROUP_CHANGED"])
        # 同一来源每窗口只出现一次
        self.assertEqual(len(contrib["contributions"]), 2)

    def test_causality_correction_moves_exposure(self):
        # 在定义中排除 UNRELATED：因果评价从可能->无关后，来源从药品组撤出
        spec = {
            "drug_groups": [{"id": "D_IBU", "drug_codes": ["IBU"]},
                            {"id": "D_AMX", "drug_codes": ["AMX"]}],
            "event_groups": [{"id": "E_RASH", "concept_codes": ["RASH"]},
                             {"id": "E_GI", "concept_codes": ["NAU"]}],
            "thresholds": {"min_cases": 2, "min_prr": 2.0, "min_ror": 2.0},
            "excluded_causality_levels": ["UNRELATED"],
        }
        self.app.definitions.new_version(
            self.DEFINITION_ID, spec, term_catalog_id=self.CATALOG_ID)
        self.clock.advance(days=20)
        # 新版本定义 + 因果修正同时进入 W2
        self.followup("r1", [("布洛芬", "无关")], ["皮疹"])
        run2 = self.run_of(self.submit_full(self.W2, definition_version_no=2))
        row = next(c for c in self.app.results.run_cases(run2["id"])
                   if c["source_id"] == "r1")
        self.assertEqual(row["drug_group_ids"], [])
        self.assertEqual(row["event_group_ids"], ["E_RASH"])
        self.assertIn("UNRELATED", row["causality_levels"])

    def test_merge_relinks_case_without_double_count(self):
        case_of = {s["id"]: s["case_id"] for s in self.app.sources.list_sources()}
        c_r1, c_r2 = case_of["r1"], case_of["r2"]
        # W1 冻结在归并之前（as-of 归属 = 原病例）
        before = next(c for c in self.app.results.run_cases(self.run1["id"])
                      if c["source_id"] == "r2")["case_id"]
        self.assertEqual(before, c_r2)
        self.clock.advance(days=20)
        self.app.sources.merge_cases(c_r1, c_r2)
        # r2 归属到 c_r1
        info = self.app.sources.get_source("r2")
        self.assertEqual(info["case_id"], c_r1)
        # 新窗口：r2 病例变了，但指纹（统计贡献）不变 -> 指标不变
        run2 = self.run_of(self.submit_full(self.W2))
        self.assertEqual(self.pair(run2, "D_IBU", "E_RASH")["a"], 8)
        # 贡献追踪：W1 归属原病例（as-of），W2 记录 CASE_RELINK
        contrib = self.app.results.source_contributions("r2")
        entries = contrib["contributions"]
        self.assertEqual(entries[0]["case_id"], c_r2)
        self.assertEqual(entries[1]["case_id"], c_r1)
        self.assertIn("CASE_RELINK", entries[1]["change_from_prior"])

    def test_split_then_merge_lineage(self):
        case_of = {s["id"]: s["case_id"] for s in self.app.sources.list_sources()}
        c_r3 = case_of["r3"]
        sp = self.app.sources.split_source("r3")
        self.assertEqual(sp["old_case_id"], c_r3)
        self.assertNotEqual(sp["new_case_id"], c_r3)
        # 再合回去
        self.app.sources.merge_cases(c_r3, sp["new_case_id"])
        self.assertEqual(self.app.sources.get_source("r3")["case_id"], c_r3)

    def test_withdrawal_removes_from_next_window(self):
        self.clock.advance(days=20)
        self.app.sources.add_version(
            "r1", {"drugs": [], "events": []}, change_type="WITHDRAWAL")
        run2 = self.run_of(self.submit_full(self.W2))
        ids = {c["source_id"] for c in self.app.results.run_cases(run2["id"])}
        self.assertNotIn("r1", ids)
        self.assertEqual(self.pair(run2, "D_IBU", "E_RASH")["a"], 7)
        diff = self.app.results.diff_runs(self.run1["id"], run2["id"])
        removed = {c["source_id"] for c in diff["case_changes"]["removed"]}
        self.assertIn("r1", removed)
        # 旧窗口快照仍含 r1（不被后台覆盖）
        old_ids = {c["source_id"]
                   for c in self.app.results.run_cases(self.run1["id"])}
        self.assertIn("r1", old_ids)

    def test_revision_diff_field_level(self):
        rev1 = self.pair(self.run1, "D_IBU", "E_RASH")["revision_id"]
        self.clock.advance(days=20)
        self.followup("r1", ["布洛芬"], ["恶心"])
        run2 = self.run_of(self.submit_full(self.W2))
        rev2 = self.pair(run2, "D_IBU", "E_RASH")["revision_id"]
        d = self.app.results.revision_diff(rev1, rev2)
        self.assertEqual(d["count_fields"]["a"], {"old": 8, "new": 7})
        self.assertIn("prr", d["metric_fields"])
        self.assertFalse(d["decision_changed"])  # 仍是 SIGNAL

    def test_new_source_appears_in_diff(self):
        self.clock.advance(days=20)
        self.source("n9", ["布洛芬"], ["皮疹"], "2026-09-25")
        run2 = self.run_of(self.submit_full(self.W2))
        diff = self.app.results.diff_runs(self.run1["id"], run2["id"])
        added = {c["source_id"] for c in diff["case_changes"]["added"]}
        self.assertIn("n9", added)

    def test_source_one_contribution_per_window_after_repeated_followups(self):
        self.clock.advance(days=5)
        self.followup("r1", ["布洛芬"], ["皮疹"])
        self.clock.advance(days=5)
        self.followup("r1", ["布洛芬", "阿莫西林"], ["皮疹"])
        run2 = self.run_of(self.submit_full(self.W2))
        rows = [c for c in self.app.results.run_cases(run2["id"])
                if c["source_id"] == "r1"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["version_no"], 3)


class TestMergeSplitConcurrency(BaseSignalTest):
    def test_concurrent_merges_keep_single_active_membership(self):
        # 8 个独立病例的来源；多线程并发把它们全部归并进 r1 病例
        for i in range(1, 9):
            self.source(f"r{i}", ["布洛芬"], ["皮疹"], f"2026-09-{i:02d}")
        case_of = {s["id"]: s["case_id"]
                   for s in self.app.sources.list_sources()}
        survivor = case_of["r1"]
        others = [case_of[f"r{i}"] for i in range(2, 9)]

        def merge(other):
            try:
                self.app.sources.merge_cases(survivor, other)
                return True
            except Exception:
                return False
        with concurrent.futures.ThreadPoolExecutor(8) as ex:
            results = list(ex.map(merge, others))
        # 全部归并完成（数据库写锁串行化，无丢归属）
        self.assertTrue(all(results))
        conn = sig_connect(self.app.db_path)
        try:
            active = conn.execute(
                "SELECT COUNT(*) AS n FROM sig_case_links WHERE active=1"
            ).fetchone()["n"]
            total = conn.execute(
                "SELECT COUNT(*) AS n FROM sig_sources WHERE active=1"
            ).fetchone()["n"]
            self.assertEqual(active, total)  # 每来源恰好一条 active 归属
            survivors = {r["case_id"] for r in conn.execute(
                "SELECT case_id FROM sig_case_links WHERE active=1")}
            self.assertEqual(survivors, {survivor})
        finally:
            conn.close()

    def test_concurrent_split_same_source_single_winner(self):
        self.source("r1", ["布洛芬"], ["皮疹"], "2026-09-01")
        self.source("r2", ["布洛芬"], ["皮疹"], "2026-09-02")
        case_of = {s["id"]: s["case_id"]
                   for s in self.app.sources.list_sources()}
        old_case = case_of["r1"]

        def split(_):
            # 所有并发请求基于同一个工作台视图（expected_case_id）：
            # 只有第一个拆解成功，其余收到 CASE_MOVED 让用户刷新。
            try:
                self.app.sources.split_source("r1", expected_case_id=old_case)
                return "ok"
            except Exception as e:
                return getattr(e, "code", "ERR")
        with concurrent.futures.ThreadPoolExecutor(8) as ex:
            out = list(ex.map(split, range(4)))
        self.assertEqual(out.count("ok"), 1)
        self.assertEqual(sorted(set(out)), ["CASE_MOVED", "ok"])
        conn = sig_connect(self.app.db_path)
        try:
            n = conn.execute(
                "SELECT COUNT(*) AS n FROM sig_case_links "
                "WHERE source_id='r1' AND active=1").fetchone()["n"]
            self.assertEqual(n, 1)
            n2 = conn.execute(
                "SELECT COUNT(*) AS n FROM sig_case_links "
                "WHERE source_id='r2' AND active=1").fetchone()["n"]
            self.assertEqual(n2, 1)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
